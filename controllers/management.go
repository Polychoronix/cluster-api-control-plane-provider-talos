// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/external"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func tcpManagementAuthority(tcp *controlplanev1.TalosControlPlane) op.ManagementAuthority {
	return op.ManagementAuthority{Namespace: tcp.Namespace, APIVersion: controlplanev1.GroupVersion.String(), Kind: "TalosControlPlane", Name: tcp.Name, UID: tcp.UID}
}

func (r *TalosControlPlaneReconciler) managementRequest(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster) (op.ManagementObservationRequest, error) {
	request := op.ManagementObservationRequest{Authority: tcpManagementAuthority(tcp), Cluster: cluster, Spec: tcp.Spec.Management}
	if r.APIReader == nil || request.Spec == nil {
		return request, fmt.Errorf("management observation requires APIReader and declared inventory")
	}
	declared := make([]*clusterv1.Machine, len(request.Spec.Machines))
	for i, pin := range request.Spec.Machines {
		declared[i] = &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: pin.Name, Namespace: tcp.Namespace, UID: pin.UID}}
	}
	// Inspect pauses across the complete manifest before any incomplete-member
	// status bookkeeping. A later paused member must also stop an earlier error.
	for _, pin := range declared {
		m := &clusterv1.Machine{}
		if r.APIReader.Get(ctx, client.ObjectKeyFromObject(pin), m) != nil {
			continue
		}
		if _, paused := m.Annotations[clusterv1.PausedAnnotation]; paused {
			return request, op.ErrManagementPaused
		}
		tc := &bootstrapv1.TalosConfig{}
		if r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}, tc) == nil {
			if _, paused := tc.Annotations[clusterv1.PausedAnnotation]; paused {
				return request, op.ErrManagementPaused
			}
		}
		if infra, err := external.GetObjectFromContractVersionedRef(ctx, r.APIReader, m.Spec.InfrastructureRef, m.Namespace); err == nil {
			if _, paused := infra.GetAnnotations()[clusterv1.PausedAnnotation]; paused {
				return request, op.ErrManagementPaused
			}
		}
	}
	inventory, err := r.readDeclaredControlPlaneInventory(ctx, cluster, declared)
	if err != nil {
		return request, err
	}
	for i := range inventory.Items {
		m := &inventory.Items[i]
		config := &bootstrapv1.TalosConfig{}
		if r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}, config) != nil {
			return request, fmt.Errorf("declared TalosConfig lookup failed")
		}
		if isUpdatingInPlace(m) || isUpdatingObject(config) {
			return request, fmt.Errorf("declared inventory has an unfinished update")
		}
		desired, err := tcp.Spec.EffectiveTalosConfigSpec(m)
		if err != nil {
			return request, err
		}
		effective := config.DeepCopy()
		effective.Spec = *desired
		effective.Spec.Management = nil
		request.EffectiveTalosConfigs = append(request.EffectiveTalosConfigs, effective)
		request.Machines = append(request.Machines, m)
		request.TalosConfigs = append(request.TalosConfigs, config)
	}
	return request, nil
}

// reconcileManagement is called before finalizers, deletion, identity ownership,
// bootstrap and etcd audit. Observe publishes only private snapshots/status, and
// does not certify operational lifecycle ownership through readiness conditions.
func (r *TalosControlPlaneReconciler) reconcileManagement(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster) (bool, ctrl.Result, error) {
	if tcp.Spec.Management == nil {
		if err := op.RequireNoManagementRecord(ctx, r.APIReader, tcpManagementAuthority(tcp)); err != nil {
			return true, ctrl.Result{RequeueAfter: requeueDuration}, err
		}
		if tcp.Status.Management != nil {
			return true, ctrl.Result{RequeueAfter: requeueDuration}, fmt.Errorf("management declaration removed from observed inventory")
		}
		if err := r.validateManagedInventory(ctx, tcp, cluster); err != nil {
			if errors.Is(err, op.ErrManagementPaused) {
				return true, ctrl.Result{RequeueAfter: requeueDuration}, r.patchManagementPaused(ctx, tcp)
			}
			return true, ctrl.Result{RequeueAfter: requeueDuration}, err
		}
		if !tcp.DeletionTimestamp.IsZero() {
			if handled, result, err := r.cleanupRetiringOwner(ctx, tcp, cluster); handled || err != nil {
				return handled, result, err
			}
			if err := op.ValidateManagementRetirement(ctx, r.APIReader, tcpManagementAuthority(tcp)); err != nil {
				if errors.Is(err, op.ErrManagementPaused) {
					return true, ctrl.Result{RequeueAfter: requeueDuration}, r.patchManagementPaused(ctx, tcp)
				}
				return true, ctrl.Result{RequeueAfter: requeueDuration}, err
			}
		}
		return false, ctrl.Result{}, nil
	}
	if tcp.Spec.Management.Mode == bootstrapv1.ManagementModeManaged {
		if !tcp.DeletionTimestamp.IsZero() {
			if handled, result, err := r.cleanupRetiringOwner(ctx, tcp, cluster); handled || err != nil {
				return handled, result, err
			}
			if err := op.ValidateManagementRetirement(ctx, r.APIReader, tcpManagementAuthority(tcp)); err != nil {
				if errors.Is(err, op.ErrManagementPaused) {
					return true, ctrl.Result{RequeueAfter: requeueDuration}, r.patchManagementPaused(ctx, tcp)
				}
				return true, ctrl.Result{RequeueAfter: requeueDuration}, err
			}
			return false, ctrl.Result{}, nil
		}
		status, err := op.ReadManagementStatus(ctx, r.APIReader, tcpManagementAuthority(tcp))
		if err != nil {
			return true, ctrl.Result{RequeueAfter: requeueDuration}, err
		}
		if status.ActivatedRevision != "" {
			if err := r.validateActivatedManagement(ctx, tcp, cluster); err != nil {
				if errors.Is(err, op.ErrManagementPaused) {
					return true, ctrl.Result{RequeueAfter: requeueDuration}, r.patchManagementPaused(ctx, tcp)
				}
				return true, ctrl.Result{RequeueAfter: requeueDuration}, err
			}
			return false, ctrl.Result{}, nil
		}
	}
	request, err := r.managementRequest(ctx, tcp, cluster)
	// A paused member is a strict stop: only Paused bookkeeping may change.
	if errors.Is(err, op.ErrManagementPaused) {
		return true, ctrl.Result{RequeueAfter: requeueDuration}, r.patchManagementPaused(ctx, tcp)
	}
	helper, patchErr := patch.NewHelper(tcp, r.Client)
	if patchErr != nil {
		return true, ctrl.Result{}, patchErr
	}
	if err != nil {
		r.managementObservationFailure(tcp, "InventoryIncomplete")
		return true, ctrl.Result{RequeueAfter: requeueDuration}, r.patchManagementStatus(ctx, helper, tcp)
	}
	if tcp.Spec.Management.Mode != bootstrapv1.ManagementModeManaged {
		observation, observeErr := op.ObserveManagement(ctx, r.APIReader, request)
		if errors.Is(observeErr, op.ErrManagementPaused) {
			return true, ctrl.Result{RequeueAfter: requeueDuration}, r.patchManagementPaused(ctx, tcp)
		}
		if observeErr != nil {
			r.managementObservationFailure(tcp, "ObservationFailed")
			return true, ctrl.Result{RequeueAfter: requeueDuration}, r.patchManagementStatus(ctx, helper, tcp)
		}
		status, recordErr := op.RecordManagementObservation(ctx, r.Client, r.APIReader, observation)
		if recordErr != nil {
			r.managementObservationFailure(tcp, "SnapshotUnavailable")
			return true, ctrl.Result{RequeueAfter: requeueDuration}, r.patchManagementStatus(ctx, helper, tcp)
		}
		tcp.Status.Management = status
		summary := observation.Summary()
		conditions.Set(tcp, metav1.Condition{Type: bootstrapv1.InventoryVerifiedCondition, Status: metav1.ConditionTrue, Reason: "Observed", Message: "Complete declared inventory and private inputs verified"})
		conditions.Set(tcp, metav1.Condition{Type: bootstrapv1.ManagementActivatedCondition, Status: metav1.ConditionFalse, Reason: "Observing", Message: "Read-only observation does not authorize lifecycle actions"})
		if summary.EtcdHealthy {
			conditions.Set(tcp, metav1.Condition{Type: controlplanev1.EtcdClusterHealthyCondition, Status: metav1.ConditionTrue, Reason: controlplanev1.EtcdClusterHealthyReason})
		}
		if summary.MachinesReady {
			conditions.Set(tcp, metav1.Condition{Type: controlplanev1.ControlPlaneComponentsHealthyCondition, Status: metav1.ConditionTrue, Reason: controlplanev1.ControlPlaneComponentsHealthyReason})
		}
		conditions.Set(tcp, metav1.Condition{Type: clusterv1.ReadyCondition, Status: metav1.ConditionFalse, Reason: "Observing", Message: "Inventory is read-only; lifecycle management is not activated"})
		conditions.Set(tcp, metav1.Condition{Type: controlplanev1.AvailableCondition, Status: metav1.ConditionFalse, Reason: "Observing", Message: "Read-only inventory is not lifecycle activation"})
		return true, ctrl.Result{RequeueAfter: requeueDuration}, r.patchManagementStatus(ctx, helper, tcp)
	}
	association, activateErr := op.PrepareManagementAssociation(ctx, r.Client, r.APIReader, request)
	if errors.Is(activateErr, op.ErrManagementPaused) {
		return true, ctrl.Result{RequeueAfter: requeueDuration}, r.patchManagementPaused(ctx, tcp)
	}
	if activateErr != nil {
		r.managementObservationFailure(tcp, "ApprovalStale")
		return true, ctrl.Result{RequeueAfter: requeueDuration}, r.patchManagementStatus(ctx, helper, tcp)
	}
	for _, m := range request.Machines {
		desired := association.DesiredOwners(m.UID)
		if reflect.DeepEqual(m.OwnerReferences, desired) {
			continue
		}
		if !reflect.DeepEqual(m.OwnerReferences, association.CurrentOwners(m.UID)) {
			return true, ctrl.Result{RequeueAfter: requeueDuration}, fmt.Errorf("approved ownership association changed")
		}
		if err := r.ownerPatch(ctx, m, map[string]any{"op": "add", "path": "/metadata/ownerReferences", "value": desired}); err != nil {
			return true, ctrl.Result{RequeueAfter: requeueDuration}, err
		}
	}
	status, activateErr := op.CompleteManagementAssociation(ctx, r.Client, r.APIReader, request, association)
	if errors.Is(activateErr, op.ErrManagementPaused) {
		return true, ctrl.Result{RequeueAfter: requeueDuration}, r.patchManagementPaused(ctx, tcp)
	}
	if activateErr != nil {
		r.managementObservationFailure(tcp, "AssociationIncomplete")
		return true, ctrl.Result{RequeueAfter: requeueDuration}, r.patchManagementStatus(ctx, helper, tcp)
	}
	tcp.Status.Management = status
	conditions.Set(tcp, metav1.Condition{Type: bootstrapv1.InventoryVerifiedCondition, Status: metav1.ConditionTrue, Reason: "Verified"})
	conditions.Set(tcp, metav1.Condition{Type: bootstrapv1.ManagementActivatedCondition, Status: metav1.ConditionTrue, Reason: "Activated"})
	if err := r.patchManagementStatus(ctx, helper, tcp); err != nil {
		return true, ctrl.Result{}, err
	}
	return true, ctrl.Result{RequeueAfter: requeueDuration}, nil
}

func (r *TalosControlPlaneReconciler) managementObservationFailure(tcp *controlplanev1.TalosControlPlane, reason string) {
	conditions.Set(tcp, metav1.Condition{Type: bootstrapv1.InventoryVerifiedCondition, Status: metav1.ConditionFalse, Reason: reason, Message: "Management inventory or approval cannot be verified"})
	conditions.Set(tcp, metav1.Condition{Type: bootstrapv1.ManagementActivatedCondition, Status: metav1.ConditionFalse, Reason: reason})
	conditions.Set(tcp, metav1.Condition{Type: controlplanev1.EtcdClusterHealthyCondition, Status: metav1.ConditionUnknown, Reason: "InspectionFailed"})
	conditions.Set(tcp, metav1.Condition{Type: controlplanev1.ControlPlaneComponentsHealthyCondition, Status: metav1.ConditionUnknown, Reason: "InspectionFailed"})
	conditions.Set(tcp, metav1.Condition{Type: clusterv1.ReadyCondition, Status: metav1.ConditionFalse, Reason: reason, Message: "Lifecycle management is not activated"})
	conditions.Set(tcp, metav1.Condition{Type: controlplanev1.AvailableCondition, Status: metav1.ConditionFalse, Reason: reason, Message: "Lifecycle management is not activated"})
}

func (r *TalosControlPlaneReconciler) patchManagementPaused(ctx context.Context, tcp *controlplanev1.TalosControlPlane) error {
	helper, err := patch.NewHelper(tcp, r.Client)
	if err != nil {
		return err
	}
	conditions.Set(tcp, metav1.Condition{Type: clusterv1.PausedCondition, Status: metav1.ConditionTrue, Reason: "Paused"})
	return helper.Patch(ctx, tcp, patch.WithOwnedConditions{Conditions: []string{clusterv1.PausedCondition}})
}
func (r *TalosControlPlaneReconciler) patchManagementStatus(ctx context.Context, helper *patch.Helper, tcp *controlplanev1.TalosControlPlane) error {
	conditions.Set(tcp, metav1.Condition{Type: clusterv1.PausedCondition, Status: metav1.ConditionFalse, Reason: "NotPaused"})
	return helper.Patch(ctx, tcp, patch.WithStatusObservedGeneration{}, patch.WithOwnedConditions{Conditions: []string{bootstrapv1.InventoryVerifiedCondition, bootstrapv1.ManagementActivatedCondition, clusterv1.ReadyCondition, clusterv1.PausedCondition, controlplanev1.AvailableCondition, controlplanev1.EtcdClusterHealthyCondition, controlplanev1.ControlPlaneComponentsHealthyCondition}})
}

func (r *TalosControlPlaneReconciler) validateActivatedManagement(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster) error {
	if err := op.ValidateManagementActivation(ctx, r.APIReader, tcpManagementAuthority(tcp), tcp.Spec.Management); err != nil {
		return err
	}
	return r.validateManagedInventory(ctx, tcp, cluster)
}

func (r *TalosControlPlaneReconciler) validateManagedInventory(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster) error {
	inventory, err := r.listControlPlaneMachinesForCluster(ctx, client.ObjectKeyFromObject(cluster))
	if err != nil {
		return err
	}
	for i := range inventory.Items {
		m := &inventory.Items[i]
		if !machineIsControlledByTCP(m, tcp) {
			return fmt.Errorf("control-plane inventory contains a foreign or unassociated Machine")
		}
		config := &bootstrapv1.TalosConfig{}
		if r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}, config) != nil {
			return fmt.Errorf("managed TalosConfig lookup failed")
		}
		if tcp.Spec.Management == nil && !tcp.DeletionTimestamp.IsZero() {
			if m.Spec.Bootstrap.ConfigRef.APIGroup != bootstrapv1.GroupVersion.Group || m.Spec.Bootstrap.ConfigRef.Kind != "TalosConfig" || config.Spec.Management != nil || !controlledByMachine(config, m) {
				return fmt.Errorf("native retirement bootstrap association changed")
			}
			if _, paused := m.Annotations[clusterv1.PausedAnnotation]; paused {
				return op.ErrManagementPaused
			}
			if _, paused := config.Annotations[clusterv1.PausedAnnotation]; paused {
				return op.ErrManagementPaused
			}
			infra, err := external.GetObjectFromContractVersionedRef(ctx, r.APIReader, m.Spec.InfrastructureRef, m.Namespace)
			if err != nil || !controlledByMachine(infra, m) {
				return fmt.Errorf("native retirement infrastructure association changed")
			}
			if _, paused := infra.GetAnnotations()[clusterv1.PausedAnnotation]; paused {
				return op.ErrManagementPaused
			}
			if err := op.ValidateFreshBootstrapIntent(ctx, r.APIReader, m, config); err != nil {
				return err
			}
			continue
		}
		if err := op.ValidateManagedMachine(ctx, r.APIReader, m, config); err != nil {
			return err
		}
	}
	return nil
}

// Only fully witnessed Consumed cleanup may precede deleting-authority consent.
// Prepared/Released transactions never acquire retirement permission by deletion.
func (r *TalosControlPlaneReconciler) cleanupRetiringOwner(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster) (bool, ctrl.Result, error) {
	secret, plan, err := r.readOwnerPlan(ctx, tcp)
	if err != nil {
		return true, ctrl.Result{RequeueAfter: requeueDuration}, err
	}
	if plan == nil {
		return false, ctrl.Result{}, nil
	}
	if plan.Intent.Phase != op.OwnerConsumed {
		return true, ctrl.Result{RequeueAfter: requeueDuration}, fmt.Errorf("retirement held by unfinished owner transaction")
	}
	_, err = r.resumeOwnerPlan(ctx, tcp, cluster, secret, plan)
	if errors.Is(err, op.ErrManagementPaused) {
		return true, ctrl.Result{RequeueAfter: requeueDuration}, r.patchManagementPaused(ctx, tcp)
	}
	return true, ctrl.Result{RequeueAfter: requeueDuration}, err
}

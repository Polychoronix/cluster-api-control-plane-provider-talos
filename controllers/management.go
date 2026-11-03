// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"context"
	"errors"
	"fmt"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/patch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

func tcpManagementAuthority(tcp *controlplanev1.TalosControlPlane) op.ManagementAuthority {
	return op.ManagementAuthority{Namespace: tcp.Namespace, APIVersion: controlplanev1.GroupVersion.String(), Kind: "TalosControlPlane", Name: tcp.Name, UID: tcp.UID}
}
func (r *TalosControlPlaneReconciler) managementRequest(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster) (op.ManagementObservationRequest, error) {
	request := op.ManagementObservationRequest{Authority: tcpManagementAuthority(tcp), Cluster: cluster, Manage: tcp.Spec.Manage}
	if r.APIReader == nil {
		return request, fmt.Errorf("activation APIReader required")
	}
	inventory := &clusterv1.MachineList{}
	if err := r.APIReader.List(ctx, inventory, client.InNamespace(tcp.Namespace), client.MatchingLabels{clusterv1.ClusterNameLabel: cluster.Name}); err != nil {
		return request, err
	}
	for i := range inventory.Items {
		m := &inventory.Items[i]
		owner := metav1.GetControllerOf(m)
		if owner == nil || owner.APIVersion != controlplanev1.GroupVersion.String() || owner.Kind != "TalosControlPlane" || owner.Name != tcp.Name || owner.UID != tcp.UID {
			continue
		}
		if !m.DeletionTimestamp.IsZero() {
			return request, fmt.Errorf("activation deleting member")
		}
		if _, paused := m.Annotations[clusterv1.PausedAnnotation]; paused {
			return request, op.ErrManagementPaused
		}
		config := &bootstrapv1.TalosConfig{}
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}, config); err != nil {
			return request, err
		}
		bootstrapOwner := metav1.GetControllerOf(config)
		if bootstrapOwner == nil || bootstrapOwner.APIVersion != clusterv1.GroupVersion.String() || bootstrapOwner.Kind != "Machine" || bootstrapOwner.Name != m.Name || bootstrapOwner.UID != m.UID {
			return request, fmt.Errorf("activation bootstrap owner")
		}
		desired, err := tcp.Spec.EffectiveTalosConfigSpec(m)
		if err != nil {
			return request, err
		}
		desired.TalosEndpoint = config.Spec.TalosEndpoint
		effective := config.DeepCopy()
		effective.Spec = *desired
		request.Machines = append(request.Machines, m)
		request.TalosConfigs = append(request.TalosConfigs, config)
		request.EffectiveTalosConfigs = append(request.EffectiveTalosConfigs, effective)
	}
	if tcp.Spec.Replicas == nil || len(request.Machines) != int(*tcp.Spec.Replicas) || len(request.Machines) == 0 {
		return request, fmt.Errorf("activation complete owned inventory required")
	}
	return request, nil
}

// Observation never prevents necessary first bootstrap; normal mutations remain fenced on ActivationReady.
func (r *TalosControlPlaneReconciler) reconcileManagement(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster) (bool, ctrl.Result, error) {
	if !tcp.DeletionTimestamp.IsZero() {
		if handled, result, err := r.cleanupRetiringOwner(ctx, tcp, cluster); handled || err != nil {
			return handled, result, err
		}
		return false, ctrl.Result{}, op.ValidateManagementRetirement(ctx, r.APIReader, tcpManagementAuthority(tcp))
	}
	if state, err := op.ReadManagementStatus(ctx, r.APIReader, tcpManagementAuthority(tcp)); err == nil && state.Activated && tcp.Spec.Manage {
		if err := r.validateActivatedManagement(ctx, tcp, cluster); err == nil {
			return false, ctrl.Result{}, r.setActivationStatus(ctx, tcp, true, "Activated")
		}
	}
	request, err := r.managementRequest(ctx, tcp, cluster)
	if err != nil {
		return false, ctrl.Result{RequeueAfter: requeueDuration}, r.setActivationStatus(ctx, tcp, false, "InventoryPending")
	}
	observation, err := op.ObserveManagement(ctx, r.APIReader, request)
	if err != nil {
		return false, ctrl.Result{RequeueAfter: requeueDuration}, r.setActivationFailure(ctx, tcp, "ObservationPending", err)
	}
	if _, err = op.RecordManagementObservation(ctx, r.Client, r.APIReader, observation); err != nil {
		return true, ctrl.Result{RequeueAfter: requeueDuration}, err
	}
	if !tcp.Spec.Manage {
		return false, ctrl.Result{RequeueAfter: requeueDuration}, r.setActivationStatus(ctx, tcp, false, "ConsentAbsent")
	}
	if _, err = op.ActivateManagement(ctx, r.Client, r.APIReader, request); err != nil {
		return true, ctrl.Result{RequeueAfter: requeueDuration}, r.setActivationStatus(ctx, tcp, false, "ActivationPending")
	}
	return false, ctrl.Result{}, r.setActivationStatus(ctx, tcp, true, "Activated")
}
func (r *TalosControlPlaneReconciler) setActivationStatus(ctx context.Context, tcp *controlplanev1.TalosControlPlane, active bool, reason string) error {
	helper, err := patch.NewHelper(tcp, r.Client)
	if err != nil {
		return err
	}
	status := metav1.ConditionFalse
	if active {
		status = metav1.ConditionTrue
	}
	conditions.Set(tcp, metav1.Condition{Type: bootstrapv1.ActivationReadyCondition, Status: status, Reason: reason, ObservedGeneration: tcp.Generation})
	return helper.Patch(ctx, tcp, patch.WithStatusObservedGeneration{}, patch.WithOwnedConditions{Conditions: []string{bootstrapv1.ActivationReadyCondition}})
}

// setActivationFailure publishes why an observation did not complete. CABPT stage errors
// carry only fixed stage names, never private input, configuration bytes or transport
// detail. The log line is emitted once per distinct message so the requeue loop stays quiet.
func (r *TalosControlPlaneReconciler) setActivationFailure(ctx context.Context, tcp *controlplanev1.TalosControlPlane, reason string, cause error) error {
	message := cause.Error()
	if len(message) > 256 {
		message = message[:256]
	}
	if previous := conditions.Get(tcp, bootstrapv1.ActivationReadyCondition); previous == nil || previous.Message != message || previous.Reason != reason {
		r.Log.Info("Talos observation not complete", "taloscontrolplane", client.ObjectKeyFromObject(tcp).String(), "reason", reason, "stage", message)
	}
	helper, err := patch.NewHelper(tcp, r.Client)
	if err != nil {
		return err
	}
	conditions.Set(tcp, metav1.Condition{Type: bootstrapv1.ActivationReadyCondition, Status: metav1.ConditionFalse, Reason: reason, Message: message, ObservedGeneration: tcp.Generation})
	return helper.Patch(ctx, tcp, patch.WithStatusObservedGeneration{}, patch.WithOwnedConditions{Conditions: []string{bootstrapv1.ActivationReadyCondition}})
}
func (r *TalosControlPlaneReconciler) patchManagementPaused(ctx context.Context, tcp *controlplanev1.TalosControlPlane) error {
	helper, err := patch.NewHelper(tcp, r.Client)
	if err != nil {
		return err
	}
	conditions.Set(tcp, metav1.Condition{Type: clusterv1.PausedCondition, Status: metav1.ConditionTrue, Reason: clusterv1.PausedReason, ObservedGeneration: tcp.Generation})
	return helper.Patch(ctx, tcp, patch.WithOwnedConditions{Conditions: []string{clusterv1.PausedCondition}})
}
func (r *TalosControlPlaneReconciler) validateActivatedManagement(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster) error {
	if err := op.ValidateManagementActivation(ctx, r.APIReader, tcpManagementAuthority(tcp), tcp.Spec.Manage); err != nil {
		return err
	}
	return r.validateManagedInventory(ctx, tcp, cluster)
}
func (r *TalosControlPlaneReconciler) validateManagedInventory(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster) error {
	request, err := r.managementRequest(ctx, tcp, cluster)
	if err != nil {
		return err
	}
	for i, m := range request.Machines {
		if err := op.ValidateManagedMachine(ctx, r.APIReader, m, request.TalosConfigs[i]); err != nil {
			return err
		}
	}
	return nil
}

// Before activation, only new CAPI bootstrap is allowed: no OS/config rollout, scale-down or etcd audit.
func (r *TalosControlPlaneReconciler) reconcileInitialProvisioning(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) (ctrl.Result, error) {
	if !cluster.Spec.ControlPlaneEndpoint.IsValid() {
		return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
	}
	if len(machines.Items) == 0 {
		return r.bootControlPlane(ctx, cluster, tcp)
	}
	if !tcp.Status.Bootstrapped {
		if err := r.bootstrapCluster(ctx, cluster, tcp, machines.Items); err != nil {
			return ctrl.Result{RequeueAfter: requeueDuration}, nil
		}
		tcp.Status.Bootstrapped = true
	}
	if len(machines.Items) < int(tcp.Spec.GetReplicas()) {
		for i := range machines.Items {
			if !machines.Items[i].Status.NodeRef.IsDefined() {
				return ctrl.Result{RequeueAfter: requeueDuration}, nil
			}
		}
		return r.bootControlPlane(ctx, cluster, tcp)
	}
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

// A deleting parent never bypasses an unfinished owner transaction.
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

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
	"reflect"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"
	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	"github.com/siderolabs/talos/pkg/machinery/constants"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/controllers/external"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/annotations"
	"sigs.k8s.io/cluster-api/util/certs"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/kubeconfig"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/cluster-api/util/secret"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-control-plane-provider-talos/internal/runtimeclient"
)

const requeueDuration = 30 * time.Second

// TalosControlPlaneReconciler reconciles a TalosControlPlane object
type TalosControlPlaneReconciler struct {
	client.Client
	APIReader    client.Reader
	Log          logr.Logger
	Scheme       *runtime.Scheme
	ClusterCache clustercache.ClusterCache
	Recorder     events.EventRecorder

	// RuntimeClient calls Cluster API runtime extensions. It is nil unless the InPlaceUpdates
	// feature gate is enabled, and in-place updates are skipped when it is.
	RuntimeClient runtimeclient.Caller

	// EtcdCleanupTimeout is how long the pre-terminate hook keeps retrying etcd member removal
	// before it fails open and releases the hook anyway. Zero means defaultEtcdCleanupTimeout.
	EtcdCleanupTimeout time.Duration

	// etcdDialer overrides how Talos clients are opened for etcd operations. Nil in production,
	// where talosClientForMachine is used; tests set it to inject a fake.
	etcdDialer func(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines ...clusterv1.Machine) (etcdCalls, error)
}

func (r *TalosControlPlaneReconciler) SetupWithManager(mgr ctrl.Manager, options controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&controlplanev1.TalosControlPlane{}).
		Owns(&clusterv1.Machine{}).
		Watches(
			&clusterv1.Cluster{},
			handler.EnqueueRequestsFromMapFunc(r.ClusterToTalosControlPlane),
		).
		WithOptions(options).
		Complete(r)
}

// +kubebuilder:rbac:groups=runtime.cluster.x-k8s.io,resources=extensionconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get
// +kubebuilder:rbac:groups=core,resources=events,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;patch;update
// +kubebuilder:rbac:groups=core,resources=configmaps,namespace=kube-system,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=rbac,resources=roles,namespace=kube-system,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=rbac,resources=rolebindings,namespace=kube-system,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io;bootstrap.cluster.x-k8s.io;controlplane.cluster.x-k8s.io,resources=*,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters;clusters/status,verbs=get;list;watch
// +kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines;machines/status,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apiextensions.k8s.io,resources=customresourcedefinitions,verbs=get;list;watch

func (r *TalosControlPlaneReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, reterr error) {
	logger := r.Log.WithValues("namespace", req.Namespace, "talosControlPlane", req.Name)

	// Fetch the TalosControlPlane instance.
	tcp := &controlplanev1.TalosControlPlane{}
	if err := r.APIReader.Get(ctx, req.NamespacedName, tcp); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
	}

	// The uncached Cluster controls pauses and the aggregate authority identity.
	var cluster *clusterv1.Cluster
	var err error
	for _, ref := range tcp.OwnerReferences {
		if ref.Kind != "Cluster" {
			continue
		}
		gv, parseErr := schema.ParseGroupVersion(ref.APIVersion)
		if parseErr != nil {
			return ctrl.Result{}, fmt.Errorf("invalid owner Cluster reference")
		}
		if gv.Group != clusterv1.GroupVersion.Group {
			continue
		}
		cluster = &clusterv1.Cluster{}
		err = r.APIReader.Get(ctx, client.ObjectKey{Namespace: tcp.Namespace, Name: ref.Name}, cluster)
		if err == nil && cluster.UID != ref.UID {
			return ctrl.Result{}, fmt.Errorf("owner Cluster identity changed")
		}
		break
	}
	if err != nil {
		if !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to retrieve owner Cluster from the API Server")

			return ctrl.Result{}, err
		}

		return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
	}

	if cluster == nil {
		logger.Info("cluster Controller has not yet set OwnerRef")
		return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
	}
	logger = logger.WithValues("cluster", cluster.Name)

	if annotations.IsPaused(cluster, tcp) {
		logger.Info("reconciliation is paused for this object")
		return ctrl.Result{RequeueAfter: 20 * time.Second}, r.patchManagementPaused(ctx, tcp)
	}

	if handled, result, err := r.reconcileManagement(ctx, tcp, cluster); handled || err != nil {
		return result, err
	}
	if conditions.IsTrue(tcp, clusterv1.PausedCondition) {
		helper, err := patch.NewHelper(tcp, r.Client)
		if err != nil {
			return ctrl.Result{}, err
		}
		conditions.Set(tcp, metav1.Condition{Type: clusterv1.PausedCondition, Status: metav1.ConditionFalse, Reason: "NotPaused"})
		return ctrl.Result{RequeueAfter: requeueDuration}, helper.Patch(ctx, tcp, patch.WithOwnedConditions{Conditions: []string{clusterv1.PausedCondition}})
	}

	// Wait for the cluster infrastructure to be provisioned before creating machines.
	// The set-once initialization field is used instead of the InfrastructureReady
	// condition, as the condition can go back to false during the cluster lifecycle
	// and would deadlock the control plane bootstrap.
	if tcp.DeletionTimestamp.IsZero() && !ptr.Deref(cluster.Status.Initialization.InfrastructureProvisioned, false) && (tcp.Spec.Management == nil || tcp.Spec.Management.Mode != cabptv1.ManagementModeManaged) {
		logger.Info("cluster infra not provisioned")

		return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
	}

	// Reject stale pins and ambiguous per-node policy before deferred status,
	// deletion hooks, etcd or any other lifecycle operation can act on it.
	if tcp.DeletionTimestamp.IsZero() && len(tcp.Spec.MachineConfigOverrides) != 0 {
		if err := r.validateLiveMachineConfigPins(ctx, tcp); err != nil {
			return ctrl.Result{}, err
		}
		inventory, err := r.getControlPlaneMachinesForTCP(ctx, util.ObjectKey(cluster), tcp)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := validateMachineConfigInventory(tcp, inventory.Items); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Initialize the patch helper.
	patchHelper, err := patch.NewHelper(tcp, r.Client)
	if err != nil {
		logger.Error(err, "failed to configure the patch helper")
		return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
	}

	// Add finalizer first if not exist to avoid the race condition between init and delete
	if tcp.DeletionTimestamp.IsZero() && ensureTalosControlPlaneFinalizers(tcp) {

		// patch and return right away instead of reusing the main defer,
		// because the main defer may take too much time to get cluster status

		if err := patchTalosControlPlane(ctx, patchHelper, tcp, patch.WithStatusObservedGeneration{}); err != nil {
			logger.Error(err, "failed to add finalizer to TalosControlPlane")
			return ctrl.Result{}, err
		}

		return ctrl.Result{}, nil
	}
	// Durably activated imported inventory gets lifecycle protection even before
	// infrastructure initialization, but no provisioning/update phase bypasses it.
	if tcp.DeletionTimestamp.IsZero() && !ptr.Deref(cluster.Status.Initialization.InfrastructureProvisioned, false) {
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}
	if !tcp.DeletionTimestamp.IsZero() {
		// Retirement has separate durable authority/idle-operation validation.
		// Finalizer removal may delete TCP immediately, so never defer status writes.
		return r.reconcileDelete(ctx, cluster, tcp)
	}

	defer func() {
		r.Log.Info("attempting to set control plane status")

		// Always attempt to update status.
		if err := r.updateStatus(ctx, tcp, cluster); err != nil {
			logger.Error(err, "failed to update TalosControlPlane Status")

			reterr = kerrors.NewAggregate([]error{reterr, err})
		}

		// Always attempt to Patch the TalosControlPlane object and status after each reconciliation.
		if err := patchTalosControlPlane(ctx, patchHelper, tcp, patch.WithStatusObservedGeneration{}); err != nil {
			logger.Error(err, "failed to patch TalosControlPlane")
			reterr = kerrors.NewAggregate([]error{reterr, err})
		}

		// TODO: remove this as soon as we have a proper remote cluster cache in place.
		// Make TCP to requeue in case status is not ready, so we can check for node status without waiting for a full resync (by default 10 minutes).
		// Only requeue if we are not going in exponential backoff due to error, or if we are not already re-queueing, or if the object has a deletion timestamp.
		if reterr == nil && res.RequeueAfter <= 0 && tcp.ObjectMeta.DeletionTimestamp.IsZero() {
			if tcp.Status.ReadyReplicas == 0 || tcp.Status.ReadyReplicas < tcp.Status.Replicas {
				res = ctrl.Result{RequeueAfter: 20 * time.Second}
			}
		}

		logger.Info("successfully updated control plane status")
	}()

	secret, plan, err := r.readOwnerPlan(ctx, tcp)
	if err != nil {
		return ctrl.Result{RequeueAfter: requeueDuration}, err
	}
	if plan != nil {
		held, err := r.resumeOwnerPlan(ctx, tcp, cluster, secret, plan)
		if held || err != nil {
			// Only actual read-only health observations are allowed during owner
			// recovery; no preterminate, audit, bootstrap or lifecycle phase runs.
			if machines, readErr := r.getControlPlaneMachinesForTCP(ctx, util.ObjectKey(cluster), tcp); readErr == nil && tcp.DeletionTimestamp.IsZero() {
				_, _ = r.reconcileNodeHealth(ctx, cluster, tcp, &machines)
			}
			return ctrl.Result{RequeueAfter: requeueDuration}, err
		}
	}
	if held, err := r.ownerUpdateHeld(ctx, tcp); held {
		return ctrl.Result{RequeueAfter: requeueDuration}, err
	}

	return r.reconcile(ctx, cluster, tcp)
}

func (r *TalosControlPlaneReconciler) reconcile(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane) (res ctrl.Result, err error) {
	logger := ctrl.LoggerFrom(ctx, "cluster", cluster.Name)
	logger.Info("reconcile TalosControlPlane")

	ownedMachines, err := r.getControlPlaneMachinesForTCP(ctx, util.ObjectKey(cluster), tcp)
	if err != nil {
		logger.Error(err, "failed to retrieve control plane machines for cluster")

		return ctrl.Result{}, err
	}

	if err := validateMachineConfigInventory(tcp, ownedMachines.Items); err != nil {
		return ctrl.Result{}, err
	}

	// Machine deletion hooks are serviced before anything else in this reconcile. A Machine held
	// at the pre-terminate phase blocks its InfraMachine from being deleted, and every gate below
	// -- a missing or unresolvable infrastructure template, an unpublished control plane
	// endpoint, unhealthy nodes or etcd -- can be failing exactly when a control plane Machine is
	// on its way out. Returning ahead of the handler would park such a deletion indefinitely,
	// with the fail-open deadline never even started. Nothing the handler needs comes from those
	// gates: it works off the Machines and their status addresses alone.
	hookResult, hookErr := r.reconcileMachinePreTerminateHooks(ctx, cluster, tcp, &ownedMachines)
	if hookErr != nil {
		logger.Error(hookErr, "failed to reconcile machine pre-terminate hooks")
	}

	// Update ownerrefs on infra templates
	if err := r.reconcileExternalReference(ctx, tcp.Spec.MachineTemplate.Spec.InfrastructureRef, cluster); err != nil {
		return ctrl.Result{}, kerrors.NewAggregate([]error{hookErr, err})
	}

	// If ControlPlaneEndpoint is not set, return early
	if !cluster.Spec.ControlPlaneEndpoint.IsValid() {
		logger.Info("cluster does not yet have a ControlPlaneEndpoint defined")

		return hookResult, hookErr
	}

	if len(ownedMachines.Items) > 0 {
		machinesForConditions := make([]*clusterv1.Machine, 0, len(ownedMachines.Items))
		for i := range ownedMachines.Items {
			machinesForConditions = append(machinesForConditions, &ownedMachines.Items[i])
		}

		if err := conditions.SetAggregateCondition(
			machinesForConditions,
			tcp,
			string(clusterv1.MachineReadyCondition),
			conditions.TargetConditionType(controlplanev1.MachinesAllReadyCondition),
		); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to aggregate machine readiness conditions")
		}
	}

	var (
		errs        = hookErr
		result      = hookResult
		phaseResult ctrl.Result
	)

	// run all similar reconcile steps in the loop and pick the lowest RetryAfter, aggregate errors and check the requeue flags.
	for _, phase := range []func(context.Context, *clusterv1.Cluster, *controlplanev1.TalosControlPlane, *clusterv1.MachineList) (ctrl.Result, error){
		r.reconcileEtcdMembers,
		r.reconcileNodeHealth,
		r.reconcileConditions,
		r.reconcileKubeconfig,
		r.reconcileMachines,
	} {
		phaseResult, err = phase(ctx, cluster, tcp, &ownedMachines)
		if err != nil {
			errs = kerrors.NewAggregate([]error{errs, err})
		}

		result = util.LowestNonZeroResult(result, phaseResult)
	}

	if result.RequeueAfter != 0 {
		if err != nil {
			r.Log.Error(err, "reconcile failed", "requeue after", result.RequeueAfter.String(), "error", err.Error())
		}

		return result, nil
	}

	return result, errs
}

// ClusterToTalosControlPlane is a handler.ToRequestsFunc to be used to enqueue requests for reconciliation
// for TalosControlPlane based on updates to a Cluster.
func (r *TalosControlPlaneReconciler) ClusterToTalosControlPlane(_ context.Context, o client.Object) []ctrl.Request {
	c, ok := o.(*clusterv1.Cluster)
	if !ok {
		r.Log.Error(nil, fmt.Sprintf("expected a Cluster but got a %T", o))
		return nil
	}

	controlPlaneRef := c.Spec.ControlPlaneRef
	if controlPlaneRef.IsDefined() && controlPlaneRef.Kind == "TalosControlPlane" {
		return []ctrl.Request{{NamespacedName: client.ObjectKey{Namespace: c.Namespace, Name: controlPlaneRef.Name}}}
	}

	return nil
}

func (r *TalosControlPlaneReconciler) reconcileDelete(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane) (ctrl.Result, error) {
	// Get list of all control plane machines
	ownedMachines, err := r.getControlPlaneMachinesForTCP(ctx, util.ObjectKey(cluster), tcp)
	if err != nil {
		r.Log.Error(err, "failed to retrieve control plane machines for cluster")

		return ctrl.Result{}, err
	}

	// If no control plane machines remain, remove the finalizer
	if len(ownedMachines.Items) == 0 {
		removeTalosControlPlaneFinalizers(tcp)
		return ctrl.Result{}, r.Client.Update(ctx, tcp)
	}

	for i := range ownedMachines.Items {
		ownedMachine := &ownedMachines.Items[i]

		// The whole control plane is going away, so etcd membership is not resolved member by
		// member: removing members one at a time during a full teardown is wrong (the last one
		// has nobody to forfeit leadership to) and slow. Release the hook first so it cannot
		// wedge the cascade, exactly like KCP's reconcileDelete does with /kcp-cleanup.
		if err := r.releasePreTerminateHook(ctx, ownedMachine); err != nil {
			r.Log.Error(err, "failed to release the pre-terminate hook", "machine", ownedMachine.Name)

			return ctrl.Result{}, err
		}

		// Already deleting this machine
		if !ownedMachine.ObjectMeta.DeletionTimestamp.IsZero() {
			continue
		}
		// Submit deletion request
		if err := r.Client.Delete(ctx, ownedMachine, client.Preconditions{UID: &ownedMachine.UID, ResourceVersion: &ownedMachine.ResourceVersion}); err != nil && !apierrors.IsNotFound(err) {
			r.Log.Error(err, "failed to cleanup owned machine")
			return ctrl.Result{}, err
		}
	}

	conditions.Set(tcp, metav1.Condition{
		Type:   string(controlplanev1.ResizedCondition),
		Status: metav1.ConditionFalse,
		Reason: clusterv1.DeletingReason,
	})
	// Requeue the deletion so we can check to make sure machines got cleaned up
	return ctrl.Result{RequeueAfter: requeueDuration}, nil
}

// listControlPlaneMachinesForCluster is read-only inventory, not lifecycle consent.
func (r *TalosControlPlaneReconciler) listControlPlaneMachinesForCluster(ctx context.Context, cluster client.ObjectKey) (clusterv1.MachineList, error) {
	selector := map[string]string{
		clusterv1.ClusterNameLabel:         cluster.Name,
		clusterv1.MachineControlPlaneLabel: "",
	}

	machineList := clusterv1.MachineList{}
	if r.APIReader == nil {
		return machineList, fmt.Errorf("APIReader is required for control-plane inventory")
	}
	if err := r.APIReader.List(
		ctx,
		&machineList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels(selector),
	); err != nil {
		return machineList, err
	}

	return machineList, nil
}

// getControlPlaneMachinesForTCP only returns Machines whose controller identity
// belongs to this exact TCP. Labels and cache presence never authorize adoption.
func (r *TalosControlPlaneReconciler) getControlPlaneMachinesForTCP(ctx context.Context, cluster client.ObjectKey, tcp *controlplanev1.TalosControlPlane) (clusterv1.MachineList, error) {
	list, err := r.listControlPlaneMachinesForCluster(ctx, cluster)
	if err != nil {
		return list, err
	}
	owned := list.Items[:0]
	for i := range list.Items {
		machine := &list.Items[i]
		if machineIsControlledByTCP(machine, tcp) {
			owned = append(owned, *machine)
		}
	}
	list.Items = owned
	return list, nil
}

func machineIsControlledByTCP(machine *clusterv1.Machine, tcp *controlplanev1.TalosControlPlane) bool {
	if machine == nil || tcp == nil || tcp.UID == "" || machine.Namespace != tcp.Namespace {
		return false
	}
	owner := metav1.GetControllerOf(machine)
	if owner == nil || owner.Kind != "TalosControlPlane" || owner.Name != tcp.Name || owner.UID != tcp.UID {
		return false
	}
	gv, err := schema.ParseGroupVersion(owner.APIVersion)
	return err == nil && gv.Group == controlplanev1.GroupVersion.Group
}

func ensureTalosControlPlaneFinalizers(tcp *controlplanev1.TalosControlPlane) bool {
	changed := false

	if !controllerutil.ContainsFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizer) {
		controllerutil.AddFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizer)
		changed = true
	}

	if controllerutil.ContainsFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizerLegacy) {
		controllerutil.RemoveFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizerLegacy)
		changed = true
	}

	return changed
}

func removeTalosControlPlaneFinalizers(tcp *controlplanev1.TalosControlPlane) {
	controllerutil.RemoveFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizer)
	controllerutil.RemoveFinalizer(tcp, controlplanev1.TalosControlPlaneFinalizerLegacy)
}

// getFailureDomain will return a slice of failure domains from the cluster status.
func (r *TalosControlPlaneReconciler) getFailureDomain(_ context.Context, cluster *clusterv1.Cluster) []string {
	if cluster.Status.FailureDomains == nil {
		return nil
	}

	retList := []string{}
	for _, domain := range cluster.Status.FailureDomains {
		retList = append(retList, domain.Name)
	}
	return retList
}

func (r *TalosControlPlaneReconciler) bootControlPlane(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane) (ctrl.Result, error) {
	// Since the cloned resource should eventually have a controller ref for the Machine, we create an
	// OwnerReference here without the Controller field set
	infraCloneOwner := &metav1.OwnerReference{
		APIVersion: controlplanev1.GroupVersion.String(),
		Kind:       "TalosControlPlane",
		Name:       tcp.Name,
		UID:        tcp.UID,
	}

	machineName, err := tcp.Spec.GenerateMachineName(cluster.Name, tcp.Name)
	if err != nil {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesCreatedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.MachineGenerationFailedReason,
			Message: fmt.Sprintf("Failed to generate machine name: %v", err),
		})

		return ctrl.Result{}, err
	}

	bootstrapConfig, err := tcp.Spec.EffectiveTalosConfigSpec(&clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: machineName, Namespace: tcp.Namespace}})
	if err != nil {
		return ctrl.Result{}, err
	}

	// Clone the infrastructure template. The infrastructureRef is a CAPI v1beta2
	// ContractVersionedObjectReference, so we resolve the concrete APIVersion via
	// contract labels before delegating to external.CreateFromTemplate.
	templateObj, err := external.GetObjectFromContractVersionedRef(ctx, r.Client, tcp.Spec.MachineTemplate.Spec.InfrastructureRef, tcp.Namespace)
	if err != nil {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesCreatedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.InfrastructureTemplateCloningFailedReason,
			Message: fmt.Sprintf("Failed to retrieve infrastructure template: %v", err),
		})

		// A missing template is a user-recoverable misconfiguration; back off instead of
		// returning the error so the controller does not hot-loop at MaxConcurrentReconciles.
		if apierrors.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}

		return ctrl.Result{}, err
	}

	templateRef := corev1.ObjectReference{
		APIVersion: templateObj.GetAPIVersion(),
		Kind:       templateObj.GetKind(),
		Name:       templateObj.GetName(),
		Namespace:  templateObj.GetNamespace(),
	}
	_, infraRef, err := external.CreateFromTemplate(ctx, &external.CreateFromTemplateInput{
		Client:      r.Client,
		TemplateRef: &templateRef,
		Namespace:   tcp.Namespace,
		Name:        machineName,
		OwnerRef:    infraCloneOwner,
		ClusterName: cluster.Name,
		Labels: map[string]string{
			clusterv1.MachineControlPlaneLabel: "",
		},
	})
	if err != nil {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesCreatedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.InfrastructureTemplateCloningFailedReason,
			Message: fmt.Sprintf("Failed to clone infrastructure template: %v", err),
		})

		return ctrl.Result{}, err
	}

	// Clone the bootstrap configuration
	bootstrapRef, err := r.generateTalosConfig(ctx, tcp, machineName, bootstrapConfig)
	if err != nil {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesCreatedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.BootstrapTemplateCloningFailedReason,
			Message: fmt.Sprintf("Failed to create bootstrap configuration: %v", err),
		})

		return ctrl.Result{}, err
	}

	machine := &clusterv1.Machine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      machineName,
			Namespace: tcp.Namespace,
			Labels:    controlPlaneMachineLabelsForCluster(tcp, cluster.Name),
			// Carries the pre-terminate etcd cleanup hook, so the Machine is protected from
			// the moment it exists rather than from the next reconcile.
			Annotations: r.desiredMachineAnnotations(tcp),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(tcp, controlplanev1.GroupVersion.WithKind("TalosControlPlane")),
			},
		},
		Spec: clusterv1.MachineSpec{
			ClusterName:       cluster.Name,
			Version:           tcp.Spec.Version,
			InfrastructureRef: infraRef,
			ReadinessGates:    copyMachineReadinessGates(tcp.Spec.MachineTemplate.Spec.ReadinessGates),
			Deletion:          buildMachineDeletionSpec(tcp.Spec.MachineTemplate.Spec.Deletion),
			Bootstrap: clusterv1.Bootstrap{
				ConfigRef: *bootstrapRef,
			},
		},
	}

	failureDomains := r.getFailureDomain(ctx, cluster)
	if len(failureDomains) > 0 {
		machine.Spec.FailureDomain = failureDomains[rand.Intn(len(failureDomains))]
	}

	if err := r.Client.Create(ctx, machine); err != nil {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesCreatedCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.MachineGenerationFailedReason,
			Message: fmt.Sprintf("Failed to create machine: %v", err),
		})

		return ctrl.Result{}, errors.Wrap(err, "Failed to create machine")
	}

	return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
}

func (r *TalosControlPlaneReconciler) bootstrapCluster(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machines []clusterv1.Machine) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)

	defer cancel()

	if len(machines) == 0 {
		return fmt.Errorf("no Machines to use for bootstrap")
	}
	// Never combine endpoints with distinct TLS names or routing targets. Keep
	// the deterministic bootstrap candidate's client for its later Bootstrap RPC.
	selected := &machines[0]
	for i := range machines {
		if machines[i].Name < selected.Name {
			selected = &machines[i]
		}
	}
	var bootstrapClient *talosclient.Client
	var bootstrapCtx context.Context
	defer func() {
		if bootstrapClient != nil {
			bootstrapClient.Close() //nolint:errcheck
		}
	}()
	for i := range machines {
		machine := &machines[i]
		c, rpcCtx, err := r.talosClientForMachine(ctx, tcp, machine)
		if err != nil {
			return err
		}
		bootstrapped, err := hasEtcdData(rpcCtx, c)
		if machine == selected {
			bootstrapClient, bootstrapCtx = c, rpcCtx
		} else {
			c.Close() //nolint:errcheck
		}
		if err != nil {
			r.Log.Info("could not check for etcd data on node", "machine", machine.Name, "error", err.Error())
			continue
		}
		if bootstrapped {
			return nil
		}
	}
	if err := bootstrapClient.Bootstrap(bootstrapCtx, &machineapi.BootstrapRequest{}); err != nil && status.Code(err) != codes.AlreadyExists {
		return err
	}

	return nil
}

// hasEtcdData reports whether the node the context addresses has an etcd data directory.
func hasEtcdData(ctx context.Context, c *talosclient.Client) (bool, error) {
	list, err := c.LS(ctx, &machineapi.ListRequest{Root: "/var/lib/etcd/member"})
	if err != nil {
		return false, err
	}

	_, err = list.Recv()

	switch {
	case err == nil:
		// any entry at all means the directory exists
		return true, nil
	case errors.Is(err, io.EOF) || talosclient.StatusCode(err) == codes.Canceled:
		return false, nil
	default:
		return false, err
	}
}

func (r *TalosControlPlaneReconciler) generateTalosConfig(ctx context.Context, tcp *controlplanev1.TalosControlPlane, name string, spec *cabptv1.TalosConfigSpec) (*clusterv1.ContractVersionedObjectReference, error) {
	owner := metav1.OwnerReference{
		APIVersion:         controlplanev1.GroupVersion.String(),
		Kind:               "TalosControlPlane",
		Name:               tcp.Name,
		UID:                tcp.UID,
		BlockOwnerDeletion: ptr.To(true),
	}

	if tcp.UID == "" {
		return nil, ownerError("initial control-plane UID")
	}
	bootstrapConfig := &cabptv1.TalosConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       tcp.Namespace,
			OwnerReferences: []metav1.OwnerReference{owner},
			Annotations:     map[string]string{op.InitialControlPlaneNameAnnotation: tcp.Name, op.InitialControlPlaneUIDAnnotation: string(tcp.UID)},
		},
		Spec: *spec,
	}

	if err := r.Client.Create(ctx, bootstrapConfig); err != nil {
		return nil, errors.Wrap(err, "Failed to create bootstrap configuration")
	}

	bootstrapRef := &clusterv1.ContractVersionedObjectReference{
		APIGroup: cabptv1.GroupVersion.Group,
		Kind:     "TalosConfig",
		Name:     bootstrapConfig.GetName(),
	}

	return bootstrapRef, nil
}

func (r *TalosControlPlaneReconciler) updateStatus(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster) error {
	prepareVersionObservation(tcp)
	clusterSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{
			clusterv1.ClusterNameLabel:         cluster.Name,
			clusterv1.MachineControlPlaneLabel: "",
		},
	}

	selector, err := metav1.LabelSelectorAsSelector(clusterSelector)
	if err != nil {
		// Since we are building up the LabelSelector above, this should not fail
		return errors.Wrap(err, "failed to parse label selector")
	}
	// Copy label selector to its status counterpart in string format.
	// This is necessary for CRDs including scale subresources.
	tcp.Status.Selector = selector.String()

	ownedMachines, err := r.getControlPlaneMachinesForTCP(ctx, util.ObjectKey(cluster), tcp)
	if err != nil {
		return err
	}

	nonDeletingMachines := collections.FromMachineList(&ownedMachines).Filter(collections.Not(collections.HasDeletionTimestamp))
	replicas := int32(nonDeletingMachines.Len())

	// set basic data that does not require interacting with the workload cluster
	tcp.Status.Replicas = replicas
	tcp.Status.ReadyReplicas = 0
	tcp.Status.AvailableReplicas = ptr.To[int32](0)
	tcp.Status.UpToDateReplicas = ptr.To[int32](0)

	// Return early if the deletion timestamp is set, we don't want to try to connect to the workload cluster.
	if !tcp.DeletionTimestamp.IsZero() {
		return nil
	}

	privateHeld, privateErr := r.ownerUpdateHeld(ctx, tcp)
	_, statusPlan, _ := r.readOwnerPlan(ctx, tcp)
	var heldUID types.UID
	if statusPlan != nil {
		heldUID = statusPlan.Intent.Machine.UID
	}
	if privateHeld || privateErr != nil {
		for i := range ownedMachines.Items {
			machine := &ownedMachines.Items[i]
			if heldUID != "" && machine.UID != heldUID {
				continue
			}
			if !machine.DeletionTimestamp.IsZero() {
				continue
			}
			helper, err := patch.NewHelper(machine, r.Client)
			if err != nil {
				return err
			}
			conditions.Set(machine, metav1.Condition{Type: clusterv1.MachineUpToDateCondition, Status: metav1.ConditionFalse, Reason: clusterv1.MachineUpToDateUpdatingReason, Message: "Private owner update completion is not verified"})
			if err := helper.Patch(ctx, machine); err != nil {
				return err
			}
		}
	}
	controlPlane, err := newControlPlane(ctx, r.Client, cluster, tcp, nonDeletingMachines)
	if err != nil {
		r.Log.Info("failed to compute updated replica status", "error", err)
	} else {
		controlPlane.updateHeld = privateHeld || privateErr != nil
		controlPlane.updateMachine = heldUID

		// Set the v1beta2 UpToDate condition on each owned control plane Machine.
		// Core Machine controller skips UpToDate for stand-alone Machines, so the
		// control plane provider must compute it; otherwise dependent workloads
		// (MachineDeployment rollout gating, Cluster status aggregation) stall.
		if err := controlPlane.reconcileMachineUpToDateConditions(ctx, r.Client); err != nil {
			r.Log.Info("failed to reconcile machine UpToDate conditions", "error", err)
		}
	}

	// Count replica states from owned Machine conditions. This is the source of truth and
	// is independent of workload-cluster reachability, so AvailableReplicas / UpToDateReplicas /
	// ReadyReplicas remain consistent with Replicas even when the workload API is unreachable.
	var availableReplicas, upToDateReplicas, readyReplicas int32
	for i := range ownedMachines.Items {
		machine := &ownedMachines.Items[i]
		if !machine.DeletionTimestamp.IsZero() {
			continue
		}
		if conditions.IsTrue(machine, clusterv1.MachineAvailableCondition) {
			availableReplicas++
		}
		if conditions.IsTrue(machine, clusterv1.MachineUpToDateCondition) {
			upToDateReplicas++
		}
		if conditions.IsTrue(machine, clusterv1.MachineReadyCondition) {
			readyReplicas++
		}
	}

	tcp.Status.AvailableReplicas = ptr.To(availableReplicas)
	tcp.Status.UpToDateReplicas = ptr.To(upToDateReplicas)
	tcp.Status.ReadyReplicas = readyReplicas

	if tcp.Spec.Kubeconfig.Mode == controlplanev1.KubeconfigModeExternal {
		if _, err := r.reconcileExternalKubeconfig(ctx, cluster, tcp); err != nil {
			conditions.Set(tcp, metav1.Condition{Type: string(controlplanev1.AvailableCondition), Status: metav1.ConditionFalse, Reason: "ExternalKubeconfigUnavailable", Message: "Externally managed kubeconfig is not valid or available"})
			versionObservationFailed(tcp, "InspectionFailed", "External workload credentials cannot be validated")
			return nil
		}
	}
	updateHeld, _ := r.ownerUpdateHeld(ctx, tcp)
	observedVersion := tcp.Status.Version
	r.observeControlPlaneVersion(ctx, tcp, cluster, ownedMachines.Items)
	if updateHeld {
		tcp.Status.Version = observedVersion
		versionObservationFailed(tcp, "Updating", "Private owner update completion is not verified")
	}

	// Probe workload cluster reachability to set Initialized + AvailableCondition. Failure here
	// only suppresses the AvailableCondition; replica counters stay accurate.
	c, err := r.ClusterCache.GetClient(ctx, util.ObjectKey(cluster))
	if err != nil {
		if tcp.Spec.Kubeconfig.Mode == controlplanev1.KubeconfigModeExternal {
			conditions.Set(tcp, metav1.Condition{Type: string(controlplanev1.AvailableCondition), Status: metav1.ConditionFalse, Reason: "ExternalKubernetesAPIUnreachable", Message: "External kubeconfig cannot establish Kubernetes API reachability"})
		}
		if tcp.Spec.Kubeconfig.Mode != controlplanev1.KubeconfigModeExternal {
			r.Log.Info("failed to get kubeconfig for the cluster", "error", err)
		}

		return nil
	}

	nodeSelector := labels.NewSelector()
	req, err := labels.NewRequirement(constants.LabelNodeRoleControlPlane, selection.Exists, []string{})
	if err != nil {
		return err
	}

	if err := c.List(ctx, &corev1.NodeList{}, &client.ListOptions{LabelSelector: nodeSelector.Add(*req)}); err != nil {
		if tcp.Spec.Kubeconfig.Mode == controlplanev1.KubeconfigModeExternal {
			conditions.Set(tcp, metav1.Condition{Type: string(controlplanev1.AvailableCondition), Status: metav1.ConditionFalse, Reason: "ExternalKubernetesAPIUnreachable", Message: "External kubeconfig cannot establish Kubernetes API reachability"})
		}
		if tcp.Spec.Kubeconfig.Mode != controlplanev1.KubeconfigModeExternal {
			r.Log.Info("failed to list controlplane nodes", "error", err)
		}

		return nil
	}

	// if we were able to fetch some resources via control plane endpoint,
	// workload cluster control plane endpoint is available
	tcp.Status.Initialization.ControlPlaneInitialized = ptr.To(true)
	conditions.Set(tcp, metav1.Condition{
		Type:   string(controlplanev1.AvailableCondition),
		Status: metav1.ConditionTrue,
		Reason: controlplanev1.AvailableReason,
	})

	r.Log.Info("ready replicas", "count", tcp.Status.ReadyReplicas)

	return nil
}

func (r *TalosControlPlaneReconciler) reconcileExternalReference(ctx context.Context, ref clusterv1.ContractVersionedObjectReference, cluster *clusterv1.Cluster) error {
	if !strings.HasSuffix(ref.Kind, clusterv1.TemplateSuffix) {
		return nil
	}

	obj, err := external.GetObjectFromContractVersionedRef(ctx, r.Client, ref, cluster.Namespace)
	if err != nil {
		return err
	}

	objPatchHelper, err := patch.NewHelper(obj, r.Client)
	if err != nil {
		return err
	}

	obj.SetOwnerReferences(util.EnsureOwnerRef(obj.GetOwnerReferences(), metav1.OwnerReference{
		APIVersion: clusterv1.GroupVersion.String(),
		Kind:       "Cluster",
		Name:       cluster.Name,
		UID:        cluster.UID,
	}))

	return objPatchHelper.Patch(ctx, obj)
}

func (r *TalosControlPlaneReconciler) reconcileKubeconfig(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) (ctrl.Result, error) {
	if tcp.Spec.Kubeconfig.Mode == controlplanev1.KubeconfigModeExternal {
		return r.reconcileExternalKubeconfig(ctx, cluster, tcp)
	}
	endpoint := cluster.Spec.ControlPlaneEndpoint
	if endpoint.IsZero() {
		return ctrl.Result{}, nil
	}

	clusterName := util.ObjectKey(cluster)
	existingKubeconfig, err := secret.GetFromNamespacedName(ctx, r.Client, clusterName, secret.Kubeconfig)

	switch {
	case apierrors.IsNotFound(err):
		createErr := kubeconfig.CreateSecretWithOwner(
			ctx,
			r.Client,
			clusterName,
			endpoint.String(),
			*metav1.NewControllerRef(tcp, controlplanev1.GroupVersion.WithKind("TalosControlPlane")),
		)
		if createErr != nil {
			if errors.Is(createErr, kubeconfig.ErrDependentCertificateNotFound) {
				r.Log.Info("could not find secret", "secret", secret.ClusterCA, "cluster", clusterName.Name, "namespace", clusterName.Namespace)

				return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
			}

			return ctrl.Result{}, createErr
		}
	case err != nil:
		return ctrl.Result{RequeueAfter: 20 * time.Second}, fmt.Errorf("failed to retrieve kubeconfig Secret for Cluster %q in namespace %q: %w", clusterName.Name, clusterName.Namespace, err)
	default:
		// kubeconfig is already generated
		needsRotation, err := kubeconfig.NeedsClientCertRotation(existingKubeconfig, certs.ClientCertificateRenewalDuration)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to figure out if we need to regenerate cluster client cert: %w", err)
		}

		if !needsRotation {
			return ctrl.Result{}, nil
		}

		r.Log.Info("kubeconfig certificate rotation", "secret", secret.Kubeconfig, "cluster", clusterName.Name, "namespace", clusterName.Namespace)

		err = kubeconfig.RegenerateSecret(ctx, r.Client, existingKubeconfig)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to regenerate kubeconfig: %w", err)
		}
	}

	return ctrl.Result{}, nil
}

func (r *TalosControlPlaneReconciler) reconcileEtcdMembers(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) (result ctrl.Result, err error) {
	var errs error
	// Audit the etcd member list to remove any nodes that no longer exist
	if err := r.auditEtcd(ctx, tcp, util.ObjectKey(cluster)); err != nil {
		errs = kerrors.NewAggregate([]error{errs, err})
	}

	if err := r.etcdHealthcheck(ctx, tcp, machines.Items); err != nil {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.EtcdClusterHealthyCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.EtcdClusterUnhealthyReason,
			Message: fmt.Sprintf("Failed to perform etcd healthcheck: %v", err),
		})
		errs = kerrors.NewAggregate([]error{errs, err})
	} else {
		conditions.Set(tcp, metav1.Condition{
			Type:   string(controlplanev1.EtcdClusterHealthyCondition),
			Status: metav1.ConditionTrue,
			Reason: controlplanev1.EtcdClusterHealthyReason,
		})
	}

	if errs != nil {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, errs
	}

	return ctrl.Result{}, nil
}

func (r *TalosControlPlaneReconciler) reconcileNodeHealth(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) (result ctrl.Result, err error) {
	if err := r.nodesHealthcheck(ctx, tcp, healthCheckable(machines.Items)); err != nil {
		reason := controlplanev1.ControlPlaneComponentsInspectionFailedReason

		var unhealthy *errServiceUnhealthy
		if errors.As(err, &unhealthy) {
			reason = controlplanev1.ControlPlaneComponentsUnhealthyReason
		}

		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.ControlPlaneComponentsHealthyCondition),
			Status:  metav1.ConditionFalse,
			Reason:  reason,
			Message: fmt.Sprintf("Failed to perform control plane healthcheck: %v", err),
		})

		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	} else {
		conditions.Set(tcp, metav1.Condition{
			Type:   string(controlplanev1.ControlPlaneComponentsHealthyCondition),
			Status: metav1.ConditionTrue,
			Reason: controlplanev1.ControlPlaneComponentsHealthyReason,
		})
	}

	return ctrl.Result{}, nil
}

func (r *TalosControlPlaneReconciler) reconcileConditions(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) (result ctrl.Result, err error) {
	if !conditions.Has(tcp, string(controlplanev1.AvailableCondition)) {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.AvailableCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.WaitingForTalosBootReason,
			Message: "Waiting for Talos to bootstrap",
		})
	}

	if !conditions.Has(tcp, string(controlplanev1.MachinesBootstrapped)) {
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesBootstrapped),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.WaitingForMachinesReason,
			Message: "Waiting for machines to bootstrap",
		})
	}

	return ctrl.Result{}, nil
}

func (r *TalosControlPlaneReconciler) reconcileMachineTemplateState(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) error {
	desiredLabels := controlPlaneMachineLabelsForCluster(tcp, cluster.Name)
	desiredAnnotations := copyStringMap(tcp.Spec.MachineTemplate.ObjectMeta.Annotations)
	desiredReadinessGates := copyMachineReadinessGates(tcp.Spec.MachineTemplate.Spec.ReadinessGates)
	desiredDeletionSpec := buildMachineDeletionSpec(tcp.Spec.MachineTemplate.Spec.Deletion)

	for i := range machines.Items {
		machine := &machines.Items[i]
		if !machine.DeletionTimestamp.IsZero() {
			continue
		}

		patchHelper, err := patch.NewHelper(machine, r.Client)
		if err != nil {
			return err
		}

		changed := false

		if machine.Labels == nil {
			machine.Labels = map[string]string{}
		}

		for key, value := range desiredLabels {
			if machine.Labels[key] != value {
				machine.Labels[key] = value
				changed = true
			}
		}

		if len(desiredAnnotations) > 0 {
			if machine.Annotations == nil {
				machine.Annotations = map[string]string{}
			}

			for key, value := range desiredAnnotations {
				if machine.Annotations[key] != value {
					machine.Annotations[key] = value
					changed = true
				}
			}
		}

		if !reflect.DeepEqual(machine.Spec.ReadinessGates, desiredReadinessGates) {
			machine.Spec.ReadinessGates = copyMachineReadinessGates(desiredReadinessGates)
			changed = true
		}

		if !reflect.DeepEqual(machine.Spec.Deletion, desiredDeletionSpec) {
			machine.Spec.Deletion = desiredDeletionSpec
			changed = true
		}

		if !changed {
			continue
		}

		if err := patchHelper.Patch(ctx, machine); err != nil {
			return err
		}
	}

	return nil
}

func (r *TalosControlPlaneReconciler) reconcileMachines(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane, machines *clusterv1.MachineList) (res ctrl.Result, err error) {
	logger := r.Log.WithValues("namespace", tcp.Namespace, "talosControlPlane", tcp.Name)

	// The owner's spec writes precede runtime completion. Hold every Machine
	// mutation, including scaling and replacement, until the core executor clears
	// both transaction annotations. This also covers zero outdated Machines.
	updating := 0
	for i := range machines.Items {
		if isUpdatingInPlace(&machines.Items[i]) {
			updating++
		}
	}
	if updating > 0 {
		logger.Info("waiting for in-place transaction completion", "transactions", updating)
		conditions.Set(tcp, metav1.Condition{
			Type:    string(controlplanev1.MachinesSpecUpToDateCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.InPlaceUpdateInProgressReason,
			Message: fmt.Sprintf("Waiting for %d in-place transactions to complete", updating),
		})
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}
	numMachines := len(machines.Items)
	desiredReplicas := int(tcp.Spec.GetReplicas())

	controlPlane, err := newControlPlane(ctx, r.Client, cluster, tcp, collections.FromMachineList(machines))
	if err != nil {
		return ctrl.Result{}, err
	}

	if err := r.reconcileMachineTemplateState(ctx, cluster, tcp, machines); err != nil {
		return ctrl.Result{}, err
	}

	outdated, err := controlPlane.MachinesNeedingRollout()
	if err != nil {
		return ctrl.Result{}, err
	}

	// Offer outdated machines to the in-place update extension first. It claims the one it
	// is updating and names the ones that have to be replaced; everything else waits. The
	// rollout path only ever sees what it names, so a machine is never replaced while
	// another is being updated in place.
	decision, err := r.reconcileInPlaceUpdates(ctx, tcp, controlPlane, outdated)
	if err != nil {
		return ctrl.Result{}, err
	}

	if len(decision.claimed) > 0 {
		logger.Info("machines updating in place", "machines", decision.claimed.Names())
		conditions.Set(controlPlane.TCP, metav1.Condition{
			Type:    string(controlplanev1.MachinesSpecUpToDateCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.InPlaceUpdateInProgressReason,
			Message: fmt.Sprintf("Updating %d replicas in place (%d replicas have outdated specs)", len(decision.claimed), len(outdated)),
		})
		return ctrl.Result{RequeueAfter: requeueDuration}, nil
	}

	needRollout := decision.rollout

	if len(needRollout) > 0 {
		logger.Info("rolling out control plane machines", "needRollout", needRollout.Names())
		conditions.Set(controlPlane.TCP, metav1.Condition{
			Type:    string(controlplanev1.MachinesSpecUpToDateCondition),
			Status:  metav1.ConditionFalse,
			Reason:  controlplanev1.RollingUpdateInProgressReason,
			Message: fmt.Sprintf("Rolling %d replicas with outdated spec (%d replicas up to date)", len(needRollout), len(controlPlane.Machines)-len(outdated)),
		})

		return r.upgradeControlPlane(ctx, cluster, tcp, controlPlane, needRollout)
	} else if len(outdated) == 0 {
		if conditions.Has(controlPlane.TCP, string(controlplanev1.MachinesSpecUpToDateCondition)) {
			conditions.Set(controlPlane.TCP, metav1.Condition{
				Type:   string(controlplanev1.MachinesSpecUpToDateCondition),
				Status: metav1.ConditionTrue,
				Reason: controlplanev1.MachinesSpecUpToDateReason,
			})
		}
	}

	switch {
	// We are creating the first replica
	case numMachines < desiredReplicas && numMachines == 0:
		// Create new Machine w/ init
		logger.Info("initializing control plane", "Desired", desiredReplicas, "Existing", numMachines)

		return r.bootControlPlane(ctx, cluster, tcp)
	// We are scaling up
	case numMachines < desiredReplicas && numMachines > 0:
		return r.scaleUpControlPlane(ctx, cluster, tcp, controlPlane)
	// We are scaling down
	case numMachines > desiredReplicas:
		res, err = r.scaleDownControlPlane(ctx, tcp, controlPlane, collections.Machines{})
		if err != nil {
			if res.RequeueAfter > 0 {
				logger.Info("failed to scale down control plane", "error", err)

				return res, nil
			}
		}

		return res, err
	default:
		if !tcp.Status.Bootstrapped {
			if err := r.bootstrapCluster(ctx, tcp, machines.Items); err != nil {
				conditions.Set(tcp, metav1.Condition{
					Type:    string(controlplanev1.MachinesBootstrapped),
					Status:  metav1.ConditionFalse,
					Reason:  controlplanev1.WaitingForTalosBootReason,
					Message: fmt.Sprintf("Failed to bootstrap cluster: %v", err),
				})

				logger.Info("bootstrap failed, retrying in 20 seconds", "error", err)

				return ctrl.Result{RequeueAfter: time.Second * 20}, nil
			}

			conditions.Set(tcp, metav1.Condition{
				Type:   string(controlplanev1.MachinesBootstrapped),
				Status: metav1.ConditionTrue,
				Reason: controlplanev1.MachinesBootstrappedReason,
			})

			tcp.Status.Bootstrapped = true
		}

		if conditions.Has(tcp, string(controlplanev1.MachinesAllReadyCondition)) {
			conditions.Set(tcp, metav1.Condition{
				Type:   string(controlplanev1.ResizedCondition),
				Status: metav1.ConditionTrue,
				Reason: controlplanev1.ResizedReason,
			})
		}

		conditions.Set(tcp, metav1.Condition{
			Type:   string(controlplanev1.MachinesCreatedCondition),
			Status: metav1.ConditionTrue,
			Reason: controlplanev1.MachinesCreatedReason,
		})
	}

	return ctrl.Result{}, nil
}

func controlPlaneMachineLabelsForCluster(tcp *controlplanev1.TalosControlPlane, clusterName string) map[string]string {
	labels := copyStringMap(tcp.Spec.MachineTemplate.ObjectMeta.Labels)
	if labels == nil {
		labels = map[string]string{}
	}

	labels[clusterv1.ClusterNameLabel] = clusterName
	labels[clusterv1.MachineControlPlaneLabel] = ""
	labels[clusterv1.MachineControlPlaneNameLabel] = mustFormatValue(tcp.Name)

	return labels
}

func mustFormatValue(str string) string {
	if len(validation.IsValidLabelValue(str)) == 0 {
		return str
	}

	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(str))

	return fmt.Sprintf("%x", hasher.Sum32())
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}

	return out
}

func copyMachineReadinessGates(in []clusterv1.MachineReadinessGate) []clusterv1.MachineReadinessGate {
	if len(in) == 0 {
		return nil
	}

	out := make([]clusterv1.MachineReadinessGate, len(in))
	copy(out, in)

	return out
}

func buildMachineDeletionSpec(deletion controlplanev1.TalosControlPlaneMachineTemplateDeletionSpec) clusterv1.MachineDeletionSpec {
	return clusterv1.MachineDeletionSpec{
		NodeDrainTimeoutSeconds:        copyInt32Pointer(deletion.NodeDrainTimeoutSeconds),
		NodeVolumeDetachTimeoutSeconds: copyInt32Pointer(deletion.NodeVolumeDetachTimeoutSeconds),
		NodeDeletionTimeoutSeconds:     copyInt32Pointer(deletion.NodeDeletionTimeoutSeconds),
	}
}

func copyInt32Pointer(in *int32) *int32 {
	if in == nil {
		return nil
	}

	out := *in
	return &out
}

func patchTalosControlPlane(ctx context.Context, patchHelper *patch.Helper, tcp *controlplanev1.TalosControlPlane, opts ...patch.Option) error {
	// Always update the readyCondition by summarizing the state of other conditions.
	if tcp.Spec.Kubeconfig.Mode == controlplanev1.KubeconfigModeExternal && !conditions.IsTrue(tcp, controlplanev1.KubeconfigAvailableCondition) {
		conditions.Set(tcp, metav1.Condition{Type: string(controlplanev1.AvailableCondition), Status: metav1.ConditionFalse, Reason: "ExternalKubeconfigUnavailable", Message: "Externally managed kubeconfig is not valid or available"})
	}
	if err := conditions.SetSummaryCondition(
		tcp,
		tcp,
		clusterv1.ReadyCondition,
		conditions.ForConditionTypes{
			string(controlplanev1.MachinesCreatedCondition),
			string(controlplanev1.ResizedCondition),
			string(controlplanev1.MachinesAllReadyCondition),
			string(controlplanev1.AvailableCondition),
			string(controlplanev1.MachinesBootstrapped),
		},
	); err != nil {
		return errors.Wrap(err, "failed to set summary Ready condition")
	}

	opts = append(opts,
		patch.WithOwnedConditions{Conditions: []string{
			string(controlplanev1.MachinesCreatedCondition),
			string(clusterv1.ReadyCondition),
			string(controlplanev1.ResizedCondition),
			string(controlplanev1.MachinesAllReadyCondition),
			string(controlplanev1.AvailableCondition),
			string(controlplanev1.MachinesBootstrapped),
			controlplanev1.KubeconfigAvailableCondition,
		}},
	)

	// Patch the object, ignoring conflicts on the conditions owned by this controller.
	return patchHelper.Patch(
		ctx,
		tcp,
		opts...,
	)
}

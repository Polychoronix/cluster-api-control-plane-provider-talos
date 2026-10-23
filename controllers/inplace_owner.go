// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/google/uuid"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kjson "k8s.io/apimachinery/pkg/util/json"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimev1 "sigs.k8s.io/cluster-api/api/runtime/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const ownerPlanKey = "plan"

// This private payload freezes only owner-controlled API state. Runtime execution
// remains CABPT's responsibility; neither record is reconstructed from new intent.
type ownerPlan struct {
	Intent                op.OwnerPlanIntent                   `json:"intent"`
	TCP                   controlplanev1.TalosControlPlaneSpec `json:"tcp"`
	ClusterUID            types.UID                            `json:"clusterUID"`
	ClusterOwners         []metav1.OwnerReference              `json:"clusterOwners"`
	ClusterSpec           clusterv1.ClusterSpec                `json:"clusterSpec"`
	CurrentMachine        clusterv1.MachineSpec                `json:"currentMachine"`
	DesiredMachine        clusterv1.MachineSpec                `json:"desiredMachine"`
	CurrentConfig         bootstrapv1.TalosConfigSpec          `json:"currentConfig"`
	DesiredConfig         bootstrapv1.TalosConfigSpec          `json:"desiredConfig"`
	InfrastructureVersion string                               `json:"infrastructureVersion"`
	InfrastructureSpec    map[string]any                       `json:"infrastructureSpec"`
	Receipt               *op.CompletionReceipt                `json:"receipt,omitempty"`
	OutputName            string                               `json:"outputName,omitempty"`
	OutputUID             types.UID                            `json:"outputUID,omitempty"`
	Configuration         []byte                               `json:"configuration,omitempty"`
}

func ownerError(stage string) error {
	return fmt.Errorf("control-plane update %s failed; intervention required", stage)
}
func ownerPlanName(tcp *controlplanev1.TalosControlPlane) string {
	return "tcp-" + string(tcp.UID) + "-inplace"
}
func ownerIdentity(group, kind string, object client.Object) op.ObjectIdentity {
	return op.ObjectIdentity{APIGroup: group, Kind: kind, Name: object.GetName(), UID: object.GetUID(), OwnerReferences: append([]metav1.OwnerReference(nil), object.GetOwnerReferences()...)}
}
func sameIdentity(object client.Object, pin op.ObjectIdentity) bool {
	return object.GetName() == pin.Name && object.GetUID() == pin.UID && reflect.DeepEqual(object.GetOwnerReferences(), pin.OwnerReferences)
}

func (r *TalosControlPlaneReconciler) readOwnerPlan(ctx context.Context, tcp *controlplanev1.TalosControlPlane) (*corev1.Secret, *ownerPlan, error) {
	if r.APIReader == nil || tcp.UID == "" {
		return nil, nil, ownerError("owner reader/UID")
	}
	secret := &corev1.Secret{}
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: tcp.Namespace, Name: ownerPlanName(tcp)}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, nil
		}
		return nil, nil, ownerError("plan lookup")
	}
	owner := metav1.GetControllerOf(secret)
	if owner == nil || owner.APIVersion != controlplanev1.GroupVersion.String() || owner.Kind != "TalosControlPlane" || owner.Name != tcp.Name || owner.UID != tcp.UID {
		return secret, nil, ownerError("plan controller")
	}
	if secret.UID == "" || secret.ResourceVersion == "" || secret.Annotations[op.ProvenanceAnnotation] != op.OwnerIntentProvenanceValue || !metav1.IsControlledBy(secret, tcp) {
		return secret, nil, ownerError("plan provenance")
	}
	plan := &ownerPlan{}
	var intent op.OwnerPlanIntent
	if kjson.Unmarshal(secret.Data[ownerPlanKey], plan) != nil || json.Unmarshal(secret.Data[op.OwnerIntentKey], &intent) != nil || !reflect.DeepEqual(intent, plan.Intent) {
		return secret, nil, ownerError("plan decode")
	}
	if intent.Version != op.OwnerIntentVersion || intent.TransactionID == "" || !sameIdentity(tcp, intent.Owner) || intent.Owner.APIGroup != controlplanev1.GroupVersion.Group || intent.Owner.Kind != "TalosControlPlane" || len(intent.ControlPlane) == 0 || intent.Machine.UID == "" || intent.TalosConfig.UID == "" || intent.Infrastructure.UID == "" {
		return secret, nil, ownerError("plan identity")
	}
	if intent.Phase != op.OwnerPrepared && intent.Phase != op.OwnerReleased && intent.Phase != op.OwnerConsumed {
		return secret, nil, ownerError("plan phase")
	}
	if intent.Phase != op.OwnerPrepared && len(intent.InputFreshness) == 0 {
		return secret, nil, ownerError("plan freshness")
	}
	return secret, plan, nil
}

func (r *TalosControlPlaneReconciler) writeOwnerPlan(ctx context.Context, secret *corev1.Secret, plan *ownerPlan) error {
	old := &ownerPlan{}
	if kjson.Unmarshal(secret.Data[ownerPlanKey], old) != nil {
		return ownerError("plan transition")
	}
	frozenOld, frozenNew := *old, *plan
	frozenOld.Intent.Phase, frozenNew.Intent.Phase = "", ""
	frozenOld.Intent.InputFreshness, frozenNew.Intent.InputFreshness = nil, nil
	frozenOld.Receipt, frozenNew.Receipt = nil, nil
	if old.Intent.Phase == op.OwnerPrepared {
		frozenNew.OutputName = ""
		frozenNew.OutputUID = ""
		frozenNew.Configuration = nil
	}
	if !reflect.DeepEqual(frozenOld, frozenNew) || (old.Intent.Phase == op.OwnerReleased && plan.Intent.Phase != op.OwnerConsumed) || (old.Intent.Phase == op.OwnerPrepared && plan.Intent.Phase != op.OwnerReleased) || old.Intent.Phase == op.OwnerConsumed {
		return ownerError("frozen plan transition")
	}
	if old.Intent.Phase != op.OwnerPrepared && !bytes.Equal(old.Intent.InputFreshness, plan.Intent.InputFreshness) {
		return ownerError("released freshness changed")
	}
	payload, err := json.Marshal(plan)
	if err != nil {
		return ownerError("plan encoding")
	}
	intent, err := json.Marshal(plan.Intent)
	if err != nil {
		return ownerError("intent encoding")
	}
	next := make(map[string][]byte, len(secret.Data)+2)
	for key, value := range secret.Data {
		next[key] = value
	}
	next[ownerPlanKey], next[op.OwnerIntentKey] = payload, intent
	return op.PatchData(ctx, r.Client, secret, next)
}

// ownerPatch is a no-create write. UID and RV tests are in the same API operation
// as controlled-field mutations; a pre-read followed by ForceOwnership is unsafe.
func (r *TalosControlPlaneReconciler) ownerPatch(ctx context.Context, object client.Object, changes ...map[string]any) error {
	if object.GetUID() == "" || object.GetResourceVersion() == "" {
		return ownerError("API snapshot")
	}
	ops := []map[string]any{{"op": "test", "path": "/metadata/uid", "value": object.GetUID()}, {"op": "test", "path": "/metadata/resourceVersion", "value": object.GetResourceVersion()}}
	ops = append(ops, changes...)
	data, err := json.Marshal(ops)
	if err != nil {
		return ownerError("API encoding")
	}
	if r.Client.Patch(ctx, object, client.RawPatch(types.JSONPatchType, data)) != nil {
		return ownerError("API compare-and-swap")
	}
	return nil
}
func annotationChanges(object client.Object, values map[string]string) []map[string]any {
	annotations := object.GetAnnotations()
	var changes []map[string]any
	if annotations == nil {
		changes = append(changes, map[string]any{"op": "add", "path": "/metadata/annotations", "value": map[string]string{}})
	}
	for key, value := range values {
		if existing, ok := annotations[key]; ok && existing == value {
			continue
		}
		path := "/metadata/annotations/" + escapeJSONPointer(key)
		changes = append(changes, map[string]any{"op": "add", "path": path, "value": value})
	}
	return changes
}
func escapeJSONPointer(value string) string {
	var result []byte
	for _, c := range []byte(value) {
		switch c {
		case '~':
			result = append(result, '~', '0')
		case '/':
			result = append(result, '~', '1')
		default:
			result = append(result, c)
		}
	}
	return string(result)
}

func (r *TalosControlPlaneReconciler) liveOwnerSnapshots(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster, plan *ownerPlan) (*controlplanev1.TalosControlPlane, *clusterv1.Cluster, error) {
	if r.APIReader == nil {
		return nil, nil, ownerError("live owner reader")
	}
	liveTCP := &controlplanev1.TalosControlPlane{}
	liveCluster := &clusterv1.Cluster{}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(tcp), liveTCP) != nil || r.APIReader.Get(ctx, client.ObjectKeyFromObject(cluster), liveCluster) != nil {
		return nil, nil, ownerError("live owner lookup")
	}
	if !sameIdentity(liveTCP, plan.Intent.Owner) || !reflect.DeepEqual(liveTCP.Spec, plan.TCP) || liveCluster.UID != plan.ClusterUID || !reflect.DeepEqual(liveCluster.Spec, plan.ClusterSpec) || !reflect.DeepEqual(liveCluster.OwnerReferences, plan.ClusterOwners) {
		return nil, nil, ownerError("live frozen owner intent changed")
	}
	return liveTCP, liveCluster, nil
}

func (r *TalosControlPlaneReconciler) ownerObjects(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster, plan *ownerPlan) (*clusterv1.Machine, *bootstrapv1.TalosConfig, *unstructured.Unstructured, error) {
	tcp, cluster, err := r.liveOwnerSnapshots(ctx, tcp, cluster, plan)
	if err != nil {
		return nil, nil, nil, err
	}
	if !reflect.DeepEqual(tcp.Spec, plan.TCP) || cluster.UID != plan.ClusterUID || !reflect.DeepEqual(cluster.Spec, plan.ClusterSpec) {
		return nil, nil, nil, ownerError("frozen owner intent changed")
	}
	var inventory clusterv1.MachineList
	if r.APIReader.List(ctx, &inventory, client.InNamespace(tcp.Namespace), client.MatchingLabels{clusterv1.ClusterNameLabel: cluster.Name, clusterv1.MachineControlPlaneLabel: ""}) != nil || len(inventory.Items) != len(plan.Intent.ControlPlane) {
		return nil, nil, nil, ownerError("control-plane inventory")
	}
	for i := range inventory.Items {
		found := false
		for _, pin := range plan.Intent.ControlPlane {
			if sameIdentity(&inventory.Items[i], pin) {
				found = true
				break
			}
		}
		if !found || !inventory.Items[i].DeletionTimestamp.IsZero() {
			return nil, nil, nil, ownerError("control-plane inventory changed")
		}
	}
	machine := &clusterv1.Machine{}
	config := &bootstrapv1.TalosConfig{}
	infra := &unstructured.Unstructured{}
	infra.SetGroupVersionKind(schema.GroupVersionKind{Group: plan.Intent.Infrastructure.APIGroup, Version: plan.InfrastructureVersion, Kind: plan.Intent.Infrastructure.Kind})
	for _, item := range []struct {
		object client.Object
		pin    op.ObjectIdentity
	}{{machine, plan.Intent.Machine}, {config, plan.Intent.TalosConfig}, {infra, plan.Intent.Infrastructure}} {
		if r.APIReader.Get(ctx, client.ObjectKey{Namespace: tcp.Namespace, Name: item.pin.Name}, item.object) != nil || !sameIdentity(item.object, item.pin) || !item.object.GetDeletionTimestamp().IsZero() {
			return nil, nil, nil, ownerError("object identity changed")
		}
	}
	currentMachine := *machine.Spec.DeepCopy()
	currentMachine.Version = plan.CurrentMachine.Version
	if !reflect.DeepEqual(currentMachine, plan.CurrentMachine) || (machine.Spec.Version != plan.CurrentMachine.Version && machine.Spec.Version != plan.DesiredMachine.Version) {
		return nil, nil, nil, ownerError("Machine spec changed")
	}
	if !reflect.DeepEqual(config.Spec, plan.CurrentConfig) && !reflect.DeepEqual(config.Spec, plan.DesiredConfig) {
		return nil, nil, nil, ownerError("TalosConfig spec changed")
	}
	infraSpec, _, _ := unstructured.NestedMap(infra.Object, "spec")
	if !reflect.DeepEqual(infraSpec, plan.InfrastructureSpec) {
		return nil, nil, nil, ownerError("InfraMachine spec changed")
	}
	return machine, config, infra, nil
}

func (r *TalosControlPlaneReconciler) createOwnerPlan(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster, update *inPlacePlan) (*corev1.Secret, *ownerPlan, error) {
	if r.APIReader == nil || tcp.UID == "" || cluster.UID == "" {
		return nil, nil, ownerError("owner reader/UID")
	}
	if update == nil || update.desiredInfraMachine == nil || update.desiredTalosConfig == nil || update.currentTalosConfig == nil {
		return nil, nil, ownerError("complete plan required")
	}
	liveTCP := &controlplanev1.TalosControlPlane{}
	liveCluster := &clusterv1.Cluster{}
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(tcp), liveTCP) != nil || r.APIReader.Get(ctx, client.ObjectKeyFromObject(cluster), liveCluster) != nil || liveTCP.UID != tcp.UID || liveCluster.UID != cluster.UID || !reflect.DeepEqual(liveTCP.Spec, tcp.Spec) || !reflect.DeepEqual(liveCluster.Spec, cluster.Spec) || !reflect.DeepEqual(liveTCP.OwnerReferences, tcp.OwnerReferences) || !reflect.DeepEqual(liveCluster.OwnerReferences, cluster.OwnerReferences) {
		return nil, nil, ownerError("CAN-approved owner changed")
	}
	tcp, cluster = liveTCP, liveCluster
	machine := &clusterv1.Machine{}
	config := &bootstrapv1.TalosConfig{}
	infra := update.desiredInfraMachine.DeepCopy()
	if r.APIReader.Get(ctx, client.ObjectKeyFromObject(update.machine), machine) != nil || r.APIReader.Get(ctx, client.ObjectKeyFromObject(update.desiredTalosConfig), config) != nil || r.APIReader.Get(ctx, client.ObjectKeyFromObject(infra), infra) != nil {
		return nil, nil, ownerError("Prepared object lookup")
	}
	if !reflect.DeepEqual(machine.Spec, update.machine.Spec) || !reflect.DeepEqual(config.Spec, update.currentTalosConfig.Spec) {
		return nil, nil, ownerError("CAN-approved snapshot changed")
	}
	if machine.UID != update.machine.UID || config.UID != update.currentTalosConfig.UID || infra.GetUID() != update.desiredInfraMachine.GetUID() || !reflect.DeepEqual(machine.OwnerReferences, update.machine.OwnerReferences) || !reflect.DeepEqual(config.OwnerReferences, update.currentTalosConfig.OwnerReferences) {
		return nil, nil, ownerError("CAN-approved identity changed")
	}
	actualInfra, _, _ := unstructured.NestedMap(infra.Object, "spec")
	approvedInfra, _, _ := unstructured.NestedMap(update.desiredInfraMachine.Object, "spec")
	if !reflect.DeepEqual(actualInfra, approvedInfra) || !reflect.DeepEqual(infra.GetOwnerReferences(), update.desiredInfraMachine.GetOwnerReferences()) || infra.GroupVersionKind() != update.desiredInfraMachine.GroupVersionKind() {
		return nil, nil, ownerError("CAN-approved infrastructure changed")
	}
	if len(machine.Annotations[runtimev1.PendingHooksAnnotation]) != 0 {
		return nil, nil, ownerError("existing pending hook")
	}
	for _, object := range []client.Object{machine, config, infra} {
		if object.GetUID() == "" || !object.GetDeletionTimestamp().IsZero() || isUpdatingObject(object) {
			return nil, nil, ownerError("existing transaction")
		}
	}
	if config.Annotations[op.OwnerPlanNameAnnotation] != "" || config.Annotations[op.OwnerPlanUIDAnnotation] != "" {
		return nil, nil, ownerError("existing plan reference")
	}
	desired := config.DeepCopy()
	desired.Spec = update.desiredTalosConfig.Spec
	resolved, err := configinput.Resolve(ctx, r.APIReader, desired)
	if err != nil {
		return nil, nil, err
	}
	gv, err := schema.ParseGroupVersion(infra.GetAPIVersion())
	if err != nil {
		return nil, nil, ownerError("infrastructure version")
	}
	plan := &ownerPlan{TCP: *tcp.Spec.DeepCopy(), ClusterUID: cluster.UID, ClusterSpec: *cluster.Spec.DeepCopy(), CurrentMachine: *machine.Spec.DeepCopy(), DesiredMachine: *update.desiredMachine.Spec.DeepCopy(), CurrentConfig: *config.Spec.DeepCopy(), DesiredConfig: *desired.Spec.DeepCopy(), InfrastructureVersion: gv.Version}
	plan.InfrastructureSpec, _, _ = unstructured.NestedMap(infra.Object, "spec")
	plan.ClusterOwners = append([]metav1.OwnerReference(nil), cluster.OwnerReferences...)
	plan.Intent = op.OwnerPlanIntent{Version: op.OwnerIntentVersion, TransactionID: uuid.NewString(), Phase: op.OwnerPrepared, Machine: ownerIdentity(clusterv1.GroupVersion.Group, "Machine", machine), TalosConfig: ownerIdentity(bootstrapv1.GroupVersion.Group, "TalosConfig", config), Infrastructure: ownerIdentity(gv.Group, infra.GetKind(), infra), Owner: ownerIdentity(controlplanev1.GroupVersion.Group, "TalosControlPlane", tcp), Inputs: resolved.Sources()}
	var list clusterv1.MachineList
	if r.APIReader.List(ctx, &list, client.InNamespace(tcp.Namespace), client.MatchingLabels{clusterv1.ClusterNameLabel: cluster.Name, clusterv1.MachineControlPlaneLabel: ""}) != nil {
		return nil, nil, ownerError("Prepared inventory")
	}
	for i := range list.Items {
		plan.Intent.ControlPlane = append(plan.Intent.ControlPlane, ownerIdentity(clusterv1.GroupVersion.Group, "Machine", &list.Items[i]))
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: ownerPlanName(tcp), Namespace: tcp.Namespace, Annotations: map[string]string{op.ProvenanceAnnotation: op.OwnerIntentProvenanceValue}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(tcp, controlplanev1.GroupVersion.WithKind("TalosControlPlane"))}}, Data: map[string][]byte{}}
	secret.Data[ownerPlanKey], err = json.Marshal(plan)
	if err != nil {
		return nil, nil, ownerError("Prepared encoding")
	}
	secret.Data[op.OwnerIntentKey], err = json.Marshal(plan.Intent)
	if err != nil {
		return nil, nil, ownerError("Prepared encoding")
	}
	if err = r.Client.Create(ctx, secret); err != nil {
		if apierrors.IsAlreadyExists(err) {
			return r.readOwnerPlan(ctx, tcp)
		}
		return nil, nil, ownerError("Prepared create")
	}
	return secret, plan, nil
}
func isUpdatingObject(object client.Object) bool {
	_, marked := object.GetAnnotations()[clusterv1.UpdateInProgressAnnotation]
	return marked
}

// resumeOwnerPlan returns a hold even when markers vanish. Only exact durable
// receipt plus complete cleanup can release it. Prepared recovery never calls Talos.
func (r *TalosControlPlaneReconciler) resumeOwnerPlan(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster, secret *corev1.Secret, plan *ownerPlan) (bool, error) {
	tcp, cluster, liveErr := r.liveOwnerSnapshots(ctx, tcp, cluster, plan)
	if liveErr != nil {
		return true, liveErr
	}
	machine, config, infra, err := r.ownerObjects(ctx, tcp, cluster, plan)
	if err != nil {
		return true, err
	}
	if !tcp.DeletionTimestamp.IsZero() || !cluster.DeletionTimestamp.IsZero() {
		return true, ownerError("owner deletion during update")
	}
	if plan.Intent.Phase == op.OwnerPrepared {
		if _, pending := machine.Annotations[runtimev1.PendingHooksAnnotation]; pending {
			return true, ownerError("pending hook before release")
		}
		resolved, err := configinput.Resolve(ctx, r.APIReader, &bootstrapv1.TalosConfig{ObjectMeta: config.ObjectMeta, Spec: plan.DesiredConfig})
		if err != nil {
			return true, err
		}
		if !reflect.DeepEqual(resolved.Sources(), plan.Intent.Inputs) {
			return true, ownerError("Prepared sources changed")
		}
		for _, object := range []client.Object{machine, infra, config} {
			values := map[string]string{clusterv1.UpdateInProgressAnnotation: ""}
			if object == config {
				values[op.OwnerPlanNameAnnotation] = secret.Name
				values[op.OwnerPlanUIDAnnotation] = string(secret.UID)
			}
			for key, value := range values {
				if old, ok := object.GetAnnotations()[key]; ok && old != value {
					return true, ownerError("controlled annotation changed")
				}
			}
			changes := annotationChanges(object, values)
			if object == machine && !reflect.DeepEqual(machine.Spec, plan.DesiredMachine) {
				changes = append(changes, map[string]any{"op": "replace", "path": "/spec/version", "value": plan.DesiredMachine.Version})
			}
			if object == config && !reflect.DeepEqual(config.Spec, plan.DesiredConfig) {
				changes = append(changes, map[string]any{"op": "replace", "path": "/spec", "value": plan.DesiredConfig})
			}
			if len(changes) > 0 {
				if err := r.ownerPatch(ctx, object, changes...); err != nil {
					return true, err
				}
			}
		}
		machine, config, infra, err = r.ownerObjects(ctx, tcp, cluster, plan)
		if err != nil {
			return true, err
		}
		if config.Status.ObservedGeneration != config.Generation || !conditions.IsTrue(config, bootstrapv1.DataSecretAvailableCondition) || config.Status.DataSecretName == "" {
			return true, nil
		}
		image := ""
		if config.Spec.ImageFactory != nil {
			condition := conditions.Get(config, bootstrapv1.ImageFactoryResolvedCondition)
			if condition == nil || condition.Status != metav1.ConditionTrue || condition.ObservedGeneration != config.Generation || config.Status.ImageFactory == nil {
				return true, nil
			}
			image = config.Status.ImageFactory.InstallerImage
		}
		output := &corev1.Secret{}
		if r.APIReader.Get(ctx, client.ObjectKey{Namespace: tcp.Namespace, Name: config.Status.DataSecretName}, output) != nil {
			return true, ownerError("rendered output lookup")
		}
		context := configinput.RenderContext{Machine: machine, Cluster: cluster, InstallerImage: image}
		fresh, err := configinput.IsFresh(ctx, r.APIReader, config, context, output)
		if err != nil {
			return true, err
		}
		if !fresh {
			return true, nil
		}
		resolved, err = configinput.Resolve(ctx, r.APIReader, config)
		if err != nil {
			return true, err
		}
		if !reflect.DeepEqual(resolved.Sources(), plan.Intent.Inputs) {
			return true, ownerError("rendered sources changed")
		}
		plan.Intent.InputFreshness, err = resolved.Freshness(context)
		if err != nil {
			return true, err
		}
		machine, config, infra, err = r.ownerObjects(ctx, tcp, cluster, plan)
		if err != nil {
			return true, err
		}
		if !isUpdatingObject(machine) || !isUpdatingObject(config) || !isUpdatingObject(infra) || config.Annotations[op.OwnerPlanNameAnnotation] != secret.Name || config.Annotations[op.OwnerPlanUIDAnnotation] != string(secret.UID) {
			return true, ownerError("release markers changed")
		}
		if machine.Annotations[runtimev1.PendingHooksAnnotation] != "" {
			return true, ownerError("premature pending hook")
		}
		finalResolved, finalErr := configinput.Resolve(ctx, r.APIReader, config)
		if finalErr != nil {
			return true, finalErr
		}
		if !reflect.DeepEqual(finalResolved.Sources(), plan.Intent.Inputs) {
			return true, ownerError("release sources changed")
		}
		finalFreshness, finalErr := finalResolved.Freshness(configinput.RenderContext{Machine: machine, Cluster: cluster, InstallerImage: image})
		if finalErr != nil {
			return true, finalErr
		}
		if !bytes.Equal(finalFreshness, plan.Intent.InputFreshness) {
			return true, ownerError("release generation changed")
		}
		if output.UID == "" || !bytes.Equal(output.Data[configinput.FreshnessKey], plan.Intent.InputFreshness) {
			return true, ownerError("rendered output identity")
		}
		plan.OutputName, plan.OutputUID, plan.Configuration = config.Status.DataSecretName, output.UID, bytes.Clone(output.Data["value"])
		plan.Intent.Phase = op.OwnerReleased
		if err := r.writeOwnerPlan(ctx, secret, plan); err != nil {
			return true, err
		}
		// This invocation won Released CAS. A resumed Released record never takes
		// this path, so a lost pending acknowledgment is not blindly reissued.
		changes := annotationChanges(machine, map[string]string{runtimev1.PendingHooksAnnotation: "UpdateMachine"})
		if err := r.ownerPatch(ctx, machine, changes...); err != nil {
			return true, err
		}
		return true, nil
	}
	if !reflect.DeepEqual(config.Spec, plan.DesiredConfig) || machine.Spec.Version != plan.DesiredMachine.Version {
		return true, ownerError("released desired state changed")
	}
	if isUpdatingInPlace(machine) || isUpdatingObject(config) || isUpdatingObject(infra) {
		if machine.Annotations[runtimev1.PendingHooksAnnotation] == "" {
			return true, ownerError("Released outcome without completion witness")
		}
		return true, nil
	}
	journalSecret, journal, err := op.ReadJournal(ctx, r.APIReader, tcp.Namespace, plan.Intent.TalosConfig)
	if err != nil {
		return true, ownerError("completion journal")
	}
	receipt, err := op.ReadReceipt(ctx, r.APIReader, tcp.Namespace, plan.Intent.TalosConfig, plan.Intent.TransactionID)
	if err != nil {
		return true, ownerError("completion receipt")
	}
	if !reflect.DeepEqual(journal.Machine, plan.Intent.Machine) || !reflect.DeepEqual(journal.TalosConfig, plan.Intent.TalosConfig) || !reflect.DeepEqual(journal.Infrastructure, plan.Intent.Infrastructure) || !reflect.DeepEqual(journal.ControlPlane, plan.Intent.ControlPlane) || !reflect.DeepEqual(journal.Inputs, plan.Intent.Inputs) || !bytes.Equal(receipt.InputFreshness, plan.Intent.InputFreshness) || journal.OwnerPlan == nil || journal.OwnerPlan.Name != secret.Name || journal.OwnerPlan.UID != secret.UID {
		return true, ownerError("completion identity mismatch")
	}
	output := &corev1.Secret{}
	if config.Status.DataSecretName != plan.OutputName {
		return true, ownerError("released output reference changed")
	}
	if r.APIReader.Get(ctx, client.ObjectKey{Namespace: tcp.Namespace, Name: config.Status.DataSecretName}, output) != nil {
		return true, ownerError("accepted intent lookup")
	}
	if output.UID != plan.OutputUID || !bytes.Equal(output.Data["value"], plan.Configuration) || !bytes.Equal(output.Data[configinput.FreshnessKey], plan.Intent.InputFreshness) {
		return true, ownerError("released output changed")
	}
	resolved, resolveErr := configinput.Resolve(ctx, r.APIReader, config)
	if resolveErr != nil {
		return true, resolveErr
	}
	if !reflect.DeepEqual(resolved.Sources(), plan.Intent.Inputs) {
		return true, ownerError("released sources changed")
	}
	if !bytes.Equal(journal.Configuration, plan.Configuration) {
		return true, ownerError("receipt configuration mismatch")
	}
	accepted, err := op.DecodeAcceptedIntent(output.Data[op.AcceptedIntentKey])
	if err != nil || accepted.TransactionID != plan.Intent.TransactionID || accepted.MachineUID != machine.UID || accepted.TalosConfigUID != config.UID || !reflect.DeepEqual(accepted.Image, journal.TargetImage) {
		return true, ownerError("accepted intent mismatch")
	}
	if plan.Intent.Phase == op.OwnerReleased {
		if err := op.ConsumeReceipt(ctx, r.Client, journalSecret, journal); err != nil {
			return true, err
		}
		if r.APIReader.Get(ctx, client.ObjectKeyFromObject(journalSecret), journalSecret) != nil {
			return true, ownerError("consumed receipt lookup")
		}
		consumed := &op.CompletionReceipt{}
		if json.Unmarshal(journalSecret.Data[op.ConsumedReceiptKey], consumed) != nil || !reflect.DeepEqual(consumed, receipt) {
			return true, ownerError("consumed receipt mismatch")
		}
		plan.Receipt = receipt
		plan.Intent.Phase = op.OwnerConsumed
		if err := r.writeOwnerPlan(ctx, secret, plan); err != nil {
			return true, err
		}
		if r.APIReader.Get(ctx, client.ObjectKeyFromObject(secret), secret) != nil {
			return true, ownerError("Consumed plan lookup")
		}
	} else if !reflect.DeepEqual(plan.Receipt, receipt) {
		return true, ownerError("Consumed receipt changed")
	}
	consumed := &op.CompletionReceipt{}
	if json.Unmarshal(journalSecret.Data[op.ConsumedReceiptKey], consumed) != nil || !reflect.DeepEqual(consumed, receipt) {
		return true, ownerError("consumed receipt mismatch")
	}
	var detach []map[string]any
	for key, want := range map[string]string{op.OwnerPlanNameAnnotation: secret.Name, op.OwnerPlanUIDAnnotation: string(secret.UID)} {
		value, exists := config.Annotations[key]
		if !exists {
			continue
		}
		if value != want {
			return true, ownerError("plan reference changed")
		}
		detach = append(detach, map[string]any{"op": "remove", "path": "/metadata/annotations/" + escapeJSONPointer(key)})
	}
	if len(detach) > 0 {
		if err := r.ownerPatch(ctx, config, detach...); err != nil {
			return true, err
		}
	}
	if err := r.Client.Delete(ctx, secret, client.Preconditions{UID: &secret.UID, ResourceVersion: &secret.ResourceVersion}); err != nil {
		return true, ownerError("Consumed plan deletion")
	}
	return false, nil
}

// ownerUpdateHeld is also used independently by deferred status reconciliation.
func (r *TalosControlPlaneReconciler) ownerUpdateHeld(ctx context.Context, tcp *controlplanev1.TalosControlPlane) (bool, error) {
	_, plan, err := r.readOwnerPlan(ctx, tcp)
	if err != nil {
		return true, err
	}
	if plan != nil {
		return true, nil
	}
	var machines clusterv1.MachineList
	clusterName := tcp.Labels[clusterv1.ClusterNameLabel]
	for _, owner := range tcp.OwnerReferences {
		if owner.Kind == "Cluster" {
			clusterName = owner.Name
			break
		}
	}
	if r.APIReader.List(ctx, &machines, client.InNamespace(tcp.Namespace), client.MatchingLabels{clusterv1.MachineControlPlaneLabel: "", clusterv1.ClusterNameLabel: clusterName}) != nil {
		return true, ownerError("transaction inventory")
	}
	for i := range machines.Items {
		machine := &machines.Items[i]
		if isUpdatingInPlace(machine) {
			return true, ownerError("operation plan missing")
		}
		config := &bootstrapv1.TalosConfig{}
		if machine.Spec.Bootstrap.ConfigRef.Kind == "TalosConfig" {
			if r.APIReader.Get(ctx, client.ObjectKey{Namespace: machine.Namespace, Name: machine.Spec.Bootstrap.ConfigRef.Name}, config) != nil {
				return true, ownerError("bootstrap transaction lookup")
			}
			if isUpdatingObject(config) || config.Annotations[op.OwnerPlanNameAnnotation] != "" || config.Annotations[op.OwnerPlanUIDAnnotation] != "" {
				return true, ownerError("operation plan missing")
			}
			journalSecret, journal, journalErr := op.ReadJournal(ctx, r.APIReader, tcp.Namespace, ownerIdentity(bootstrapv1.GroupVersion.Group, "TalosConfig", config))
			if journalErr != nil && !errors.Is(journalErr, op.ErrNotFound) {
				return true, ownerError("orphan operation journal")
			}
			if journalErr == nil {
				consumed := &op.CompletionReceipt{}
				receipt, receiptErr := op.ReadReceipt(ctx, r.APIReader, tcp.Namespace, journal.TalosConfig, journal.TransactionID)
				if receiptErr != nil || json.Unmarshal(journalSecret.Data[op.ConsumedReceiptKey], consumed) != nil || !reflect.DeepEqual(consumed, receipt) {
					return true, ownerError("operation plan missing")
				}
			}
		}
	}
	return false, nil
}

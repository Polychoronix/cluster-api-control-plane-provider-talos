// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	cp "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimev1 "sigs.k8s.io/cluster-api/api/runtime/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type ownerFixture struct {
	r       *TalosControlPlaneReconciler
	tcp     *cp.TalosControlPlane
	cluster *clusterv1.Cluster
	machine *clusterv1.Machine
	config  *bootstrapv1.TalosConfig
	infra   *unstructured.Unstructured
	update  *inPlacePlan
}

func newOwnerFixture(t *testing.T, api client.Client) *ownerFixture {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, cp.AddToScheme(scheme))
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	if api == nil {
		api = fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&clusterv1.Cluster{}, &clusterv1.Machine{}, &cp.TalosControlPlane{}, &bootstrapv1.TalosConfig{}).WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.CreateOption) error {
			if object.GetUID() == "" {
				object.SetUID(types.UID("fake-api-" + object.GetName()))
			}
			return c.Create(ctx, object, opts...)
		}}).Build()
	}
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "owner-cluster", Namespace: "default", UID: "cluster-uid"}, Spec: clusterv1.ClusterSpec{ControlPlaneEndpoint: clusterv1.APIEndpoint{Host: "localhost", Port: 6443}}}
	tcp := &cp.TalosControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: "default", UID: "owner-uid", Labels: map[string]string{clusterv1.ClusterNameLabel: cluster.Name}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cluster, clusterv1.GroupVersion.WithKind("Cluster"))}, Finalizers: []string{cp.TalosControlPlaneFinalizer}}, Spec: cp.TalosControlPlaneSpec{Version: "v1.34.0", ControlPlaneConfig: cp.ControlPlaneConfig{ControlPlaneConfig: bootstrapv1.TalosConfigSpec{GenerateType: "controlplane", ConfigSecretRef: &bootstrapv1.SecretKeyReference{Name: "owner-baseline", Key: "value"}, ConfigRevision: "next"}}}}
	machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "owner-machine", Namespace: "default", UID: "machine-uid", Labels: map[string]string{clusterv1.ClusterNameLabel: cluster.Name, clusterv1.MachineControlPlaneLabel: ""}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(tcp, cp.GroupVersion.WithKind("TalosControlPlane"))}}, Spec: clusterv1.MachineSpec{ClusterName: cluster.Name, Version: "v1.33.0", InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.cluster.x-k8s.io", Kind: "TestMachine", Name: "owner-machine"}, Bootstrap: clusterv1.Bootstrap{ConfigRef: clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: "owner-machine"}}}}
	config := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Name: machine.Name, Namespace: "default", UID: "config-uid", Labels: map[string]string{clusterv1.ClusterNameLabel: cluster.Name}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(machine, clusterv1.GroupVersion.WithKind("Machine"))}}, Spec: *tcp.Spec.ControlPlaneConfig.ControlPlaneConfig.DeepCopy()}
	config.Spec.ConfigRevision = "previous"
	infra := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "infrastructure.cluster.x-k8s.io/v1beta2", "kind": "TestMachine", "metadata": map[string]any{"name": machine.Name, "namespace": "default", "uid": "infra-uid"}, "spec": map[string]any{"identity": "preserved"}}}
	for _, object := range []client.Object{cluster, tcp, machine, config, infra, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "owner-baseline", Namespace: "default", UID: "source-uid"}, Data: map[string][]byte{"value": []byte("private-baseline-fixture")}}} {
		require.NoError(t, api.Create(t.Context(), object))
	}
	// Rebind owners after API-server identity allocation in real envtest.
	tcp.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(cluster, clusterv1.GroupVersion.WithKind("Cluster"))}
	require.NoError(t, api.Update(t.Context(), tcp))
	machine.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(tcp, cp.GroupVersion.WithKind("TalosControlPlane"))}
	require.NoError(t, api.Update(t.Context(), machine))
	config.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(machine, clusterv1.GroupVersion.WithKind("Machine"))}
	require.NoError(t, api.Update(t.Context(), config))
	r := &TalosControlPlaneReconciler{Client: api, APIReader: api, Scheme: scheme, Log: ctrl.Log}
	plane := &ControlPlane{TCP: tcp, Cluster: cluster, Machines: collections.FromMachines(machine), infraObjects: map[string]*unstructured.Unstructured{machine.Name: infra}, talosConfigs: map[string]*bootstrapv1.TalosConfig{machine.Name: config}}
	update, err := r.buildInPlacePlan(tcp, plane, machine)
	require.NoError(t, err)
	return &ownerFixture{r: r, tcp: tcp, cluster: cluster, machine: machine, config: config, infra: infra, update: update}
}

// A fixture producer supplies freshness only; these owner tests do not claim
// actual CABPT renderer/executor integration (covered separately by the harness).
func ownerOutput(t *testing.T, r *TalosControlPlaneReconciler, cluster *clusterv1.Cluster, machine *clusterv1.Machine) {
	t.Helper()
	require.NoError(t, r.APIReader.Get(t.Context(), client.ObjectKeyFromObject(machine), machine))
	config := &bootstrapv1.TalosConfig{}
	require.NoError(t, r.APIReader.Get(t.Context(), client.ObjectKey{Namespace: machine.Namespace, Name: machine.Spec.Bootstrap.ConfigRef.Name}, config))
	resolved, err := configinput.Resolve(t.Context(), r.APIReader, config)
	require.NoError(t, err)
	freshness, err := resolved.Freshness(configinput.RenderContext{Machine: machine, Cluster: cluster})
	require.NoError(t, err)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: machine.Name + "-output", Namespace: machine.Namespace, UID: "output-uid"}, Data: map[string][]byte{"value": []byte("private-rendered-fixture"), configinput.FreshnessKey: freshness}}
	existing := &corev1.Secret{}
	err = r.APIReader.Get(t.Context(), client.ObjectKeyFromObject(secret), existing)
	if err == nil {
		existing.Data = secret.Data
		require.NoError(t, r.Client.Update(t.Context(), existing))
	} else {
		require.NoError(t, r.Client.Create(t.Context(), secret))
	}
	config.Status.DataSecretName = secret.Name
	config.Status.ObservedGeneration = config.Generation
	conditions.Set(config, metav1.Condition{Type: bootstrapv1.DataSecretAvailableCondition, Status: metav1.ConditionTrue, Reason: "Available"})
	require.NoError(t, r.Client.Status().Update(t.Context(), config))
}

type faultOwnerClient struct {
	client.Client
	after int
	calls int
}

func (c *faultOwnerClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, options ...client.PatchOption) error {
	c.calls++
	err := c.Client.Patch(ctx, obj, patch, options...)
	if err == nil && c.calls == c.after {
		return fmt.Errorf("injected lost API acknowledgment")
	}
	return err
}
func TestOwnerPreparedResumesFrozenWrites(t *testing.T) {
	for boundary := range 3 {
		t.Run(fmt.Sprint(boundary), func(t *testing.T) {
			f := newOwnerFixture(t, nil)
			secret, plan, err := f.r.createOwnerPlan(t.Context(), f.tcp, f.cluster, f.update)
			require.NoError(t, err)
			api := f.r.Client
			fault := &faultOwnerClient{Client: api, after: boundary + 1}
			f.r.Client = fault
			held, err := f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, secret, plan)
			require.True(t, held)
			require.Error(t, err)
			f.r.Client = api
			secret, plan, err = f.r.readOwnerPlan(t.Context(), f.tcp)
			require.NoError(t, err)
			held, err = f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, secret, plan)
			require.NoError(t, err)
			require.True(t, held)
			ownerOutput(t, f.r, f.cluster, f.machine)
			secret, plan, err = f.r.readOwnerPlan(t.Context(), f.tcp)
			require.NoError(t, err)
			held, err = f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, secret, plan)
			require.NoError(t, err)
			require.True(t, held)
			require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.machine), f.machine))
			require.Equal(t, "v1.34.0", f.machine.Spec.Version)
			require.Contains(t, f.machine.Annotations[runtimev1.PendingHooksAnnotation], "UpdateMachine")
			_, plan, err = f.r.readOwnerPlan(t.Context(), f.tcp)
			require.NoError(t, err)
			require.Equal(t, op.OwnerReleased, plan.Intent.Phase)
		})
	}
}
func TestOwnerSourceRotationAndRecreatedObjectsStop(t *testing.T) {
	for _, stage := range []string{"source", "machine", "config", "infra", "intent"} {
		t.Run(stage, func(t *testing.T) {
			f := newOwnerFixture(t, nil)
			secret, plan, err := f.r.createOwnerPlan(t.Context(), f.tcp, f.cluster, f.update)
			require.NoError(t, err)
			switch stage {
			case "source":
				s := &corev1.Secret{}
				require.NoError(t, f.r.Client.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "owner-baseline"}, s))
				s.Data["value"] = []byte("rotation")
				require.NoError(t, f.r.Client.Update(t.Context(), s))
			case "intent":
				f.tcp.Spec.Version = "v1.35.0"
				require.NoError(t, f.r.Client.Update(t.Context(), f.tcp))
			default:
				var object client.Object
				switch stage {
				case "machine":
					object = f.machine
				case "config":
					object = f.config
				case "infra":
					object = f.infra
				}
				require.NoError(t, f.r.Client.Delete(t.Context(), object))
				object.SetResourceVersion("")
				object.SetUID("replacement")
				require.NoError(t, f.r.Client.Create(t.Context(), object))
			}
			held, err := f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, secret, plan)
			require.True(t, held)
			require.Error(t, err)
			stored := &corev1.Secret{}
			require.NoError(t, f.r.Client.Get(t.Context(), client.ObjectKeyFromObject(secret), stored))
			require.NotContains(t, err.Error(), "private")
		})
	}
}

func ownerReceipt(t *testing.T, f *ownerFixture, secret *corev1.Secret, plan *ownerPlan) {
	t.Helper()
	j := &op.Journal{Version: op.SchemaVersion, TransactionID: plan.Intent.TransactionID, Machine: plan.Intent.Machine, TalosConfig: plan.Intent.TalosConfig, Infrastructure: plan.Intent.Infrastructure, OwnerPlan: &op.OwnerPlanReference{Name: secret.Name, UID: secret.UID}, Inputs: plan.Intent.Inputs, InputFreshness: plan.Intent.InputFreshness, Configuration: []byte("private-rendered-fixture"), Phase: op.Planned, NodeName: "node", NodeUID: "node-uid", NodeProviderID: "talos://node", ControlPlane: plan.Intent.ControlPlane, Steps: []op.OperationStep{{Kind: op.Apply, Phase: op.StepPlanned, ApplyOutcome: op.Unknown}}}
	// Fixture data models the already-validated executor receipt; this is not
	// an etcd observation and is never used by production owner capture.
	for i, pin := range plan.Intent.ControlPlane {
		j.OriginalEtcdMembers = append(j.OriginalEtcdMembers, op.EtcdMemberIdentity{MemberID: uint64(i + 1), Hostname: pin.Name})
	}
	journalSecret, err := op.CreateJournal(t.Context(), f.r.Client, f.tcp.Namespace, j)
	require.NoError(t, err)
	now := metav1.Now()
	deadline := metav1.NewTime(now.Add(1000000000))
	j.Steps[0] = op.OperationStep{Kind: op.Apply, Phase: op.StepObserved, ApplyOutcome: op.NoReboot, IssuerID: "fixture", IssuedAt: &now, Deadline: &deadline, IssuedBootID: "boot"}
	j.Phase = op.Observed
	j.CompletedAt = &now
	receipt := &op.CompletionReceipt{Version: op.SchemaVersion, TransactionID: j.TransactionID, MachineUID: j.Machine.UID, TalosConfigUID: j.TalosConfig.UID, InfrastructureUID: j.Infrastructure.UID, InputFreshness: j.InputFreshness, Phase: op.Observed, CompletedAt: now}
	require.NoError(t, op.WriteJournal(t.Context(), f.r.Client, journalSecret, j, receipt))
	config := &bootstrapv1.TalosConfig{}
	require.NoError(t, f.r.Client.Get(t.Context(), client.ObjectKeyFromObject(f.config), config))
	output := &corev1.Secret{}
	require.NoError(t, f.r.Client.Get(t.Context(), client.ObjectKey{Namespace: config.Namespace, Name: config.Status.DataSecretName}, output))
	accepted, err := op.EncodeAcceptedIntent(op.AcceptedIntent{MachineUID: f.machine.UID, TalosConfigUID: f.config.UID, TransactionID: j.TransactionID, Image: j.TargetImage})
	require.NoError(t, err)
	output.Data[op.AcceptedIntentKey] = accepted
	require.NoError(t, f.r.Client.Update(t.Context(), output))
}
func cleanupCoreFlags(t *testing.T, f *ownerFixture) {
	t.Helper()
	for _, object := range []client.Object{f.machine, f.config, f.infra} {
		require.NoError(t, f.r.Client.Get(t.Context(), client.ObjectKeyFromObject(object), object))
		annotations := object.GetAnnotations()
		delete(annotations, clusterv1.UpdateInProgressAnnotation)
		delete(annotations, runtimev1.PendingHooksAnnotation)
		object.SetAnnotations(annotations)
		require.NoError(t, f.r.Client.Update(t.Context(), object))
	}
}
func releasedOwner(t *testing.T) *ownerFixture {
	t.Helper()
	f := newOwnerFixture(t, nil)
	require.NoError(t, f.r.triggerInPlaceUpdate(t.Context(), f.tcp, f.cluster, f.update))
	ownerOutput(t, f.r, f.cluster, f.machine)
	s, p, e := f.r.readOwnerPlan(t.Context(), f.tcp)
	require.NoError(t, e)
	_, e = f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
	require.NoError(t, e)
	return f
}
func TestOwnerMissingReceiptHoldsLifecycleAndStatus(t *testing.T) {
	f := releasedOwner(t)
	cleanupCoreFlags(t, f)
	s, p, e := f.r.readOwnerPlan(t.Context(), f.tcp)
	require.NoError(t, e)
	held, e := f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
	require.True(t, held)
	require.Error(t, e)
	f.cluster.Status.Initialization.InfrastructureProvisioned = new(bool)
	*f.cluster.Status.Initialization.InfrastructureProvisioned = true
	require.NoError(t, f.r.Client.Status().Update(t.Context(), f.cluster))
	f.r.ClusterCache = clustercache.NewFakeClusterCache(f.r.Client, client.ObjectKeyFromObject(f.cluster))
	calls := 0
	f.r.etcdDialer = func(context.Context, *cp.TalosControlPlane, ...clusterv1.Machine) (etcdCalls, error) {
		calls++
		return nil, fmt.Errorf("read-only observation unavailable")
	}
	_, e = f.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.tcp)})
	require.Error(t, e)
	require.NoError(t, f.r.Client.Get(t.Context(), client.ObjectKeyFromObject(f.machine), f.machine))
	require.True(t, conditions.IsFalse(f.machine, clusterv1.MachineUpToDateCondition))
	require.True(t, f.machine.DeletionTimestamp.IsZero())
	_ = calls
}
func TestOwnerReceiptConsumptionCrashRecovery(t *testing.T) {
	for boundary := range 3 {
		t.Run(fmt.Sprint(boundary), func(t *testing.T) {
			f := releasedOwner(t)
			s, p, e := f.r.readOwnerPlan(t.Context(), f.tcp)
			require.NoError(t, e)
			ownerReceipt(t, f, s, p)
			cleanupCoreFlags(t, f)
			api := f.r.Client
			f.r.Client = &faultOwnerClient{Client: api, after: boundary + 1}
			held, e := f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
			require.True(t, held)
			require.Error(t, e)
			f.r.Client = api
			s, p, e = f.r.readOwnerPlan(t.Context(), f.tcp)
			require.NoError(t, e)
			held, e = f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
			require.NoError(t, e)
			require.False(t, held)
			_, p, e = f.r.readOwnerPlan(t.Context(), f.tcp)
			require.NoError(t, e)
			require.Nil(t, p)
			require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.config), f.config))
			require.Empty(t, f.config.Annotations[op.OwnerPlanNameAnnotation])
			jSecret, j, e := op.ReadJournal(t.Context(), api, "default", sIdentity(f.config))
			require.NoError(t, e)
			var consumed op.CompletionReceipt
			require.NoError(t, json.Unmarshal(jSecret.Data[op.ConsumedReceiptKey], &consumed))
			require.Equal(t, j.TransactionID, consumed.TransactionID)
			require.True(t, reflect.DeepEqual(jSecret.Data[op.ReceiptKey], jSecret.Data[op.ConsumedReceiptKey]))
		})
	}
}
func sIdentity(config *bootstrapv1.TalosConfig) op.ObjectIdentity {
	return ownerIdentity(bootstrapv1.GroupVersion.Group, "TalosConfig", config)
}

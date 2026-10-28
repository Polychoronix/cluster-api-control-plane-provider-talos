// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilfeature "k8s.io/component-base/featuregate/testing"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimev1 "sigs.k8s.io/cluster-api/api/runtime/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/feature"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// Exercise the real owner trigger (including API-server SSA), then reconcile the
// controller with two CP candidates after the first spec already matches. No core
// executor is started: its pending hook deliberately remains uncompleted.
func TestInPlaceSerializationControllerEnvtest(t *testing.T) {
	ctx := context.Background()
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		CRDs: []*apiextensionsv1.CustomResourceDefinition{{
			ObjectMeta: metav1.ObjectMeta{Name: "testmachines.infrastructure.cluster.x-k8s.io", Labels: map[string]string{"cluster.x-k8s.io/v1beta2": "v1beta2"}},
			Spec: apiextensionsv1.CustomResourceDefinitionSpec{
				Group: "infrastructure.cluster.x-k8s.io", Scope: apiextensionsv1.NamespaceScoped,
				Names: apiextensionsv1.CustomResourceDefinitionNames{Plural: "testmachines", Singular: "testmachine", Kind: "TestMachine", ListKind: "TestMachineList"},
				Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{Name: "v1beta2", Served: true, Storage: true,
					Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"spec": {Type: "object", XPreserveUnknownFields: ptr.To(true)},
					}}},
				}},
			},
		}},
	}
	// The module directories are resolved by Go, not a user-supplied configuration path.
	for _, module := range []struct{ path, dir string }{
		{"sigs.k8s.io/cluster-api", "core/config/crd/bases"},
		{"github.com/siderolabs/cluster-api-bootstrap-provider-talos", "config/crd/bases"},
	} {
		dir := moduleDirectory(t, module.path)
		env.CRDDirectoryPaths = append(env.CRDDirectoryPaths, filepath.Join(dir, module.dir))
	}
	cfg, err := env.Start()
	if err != nil && os.Getenv("KUBEBUILDER_ASSETS") == "" && strings.Contains(err.Error(), "fork/exec") {
		t.Skipf("envtest binaries are not installed: %v", err)
	}
	require.NoError(t, err, "install local envtest binaries with setup-envtest and set KUBEBUILDER_ASSETS")
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	scheme := runtime.NewScheme()
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, cabptv1.AddToScheme(scheme))
	require.NoError(t, controlplanev1.AddToScheme(scheme))
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)

	for _, enabled := range []bool{true, false} {
		for _, replicas := range []int32{1, 2, 3} {
			for _, allMatched := range []bool{false, true} {
				t.Run(fmt.Sprintf("gate=%t/replicas=%d/allMatched=%t", enabled, replicas, allMatched), func(t *testing.T) {
					utilfeature.SetFeatureGateDuringTest(t, feature.Gates, feature.InPlaceUpdates, enabled)
					name := fmt.Sprintf("cp-%t-%d-%t", enabled, replicas, allMatched)
					f := newInPlaceFixture(t, nil)
					f.tcp.Name = name
					f.tcp.Spec.Replicas = ptr.To(replicas)
					f.tcp.Spec.RolloutStrategy = &controlplanev1.RolloutStrategy{Type: controlplanev1.RollingUpdateStrategyType, RollingUpdate: &controlplanev1.RollingUpdate{MaxSurge: ptr.To(intstr.FromInt32(1))}}
					f.tcp.Spec.MachineTemplate.Spec.InfrastructureRef = clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.cluster.x-k8s.io", Kind: "TestMachineTemplate", Name: "unused"}
					f.reconciler.Client = c
					f.reconciler.APIReader = c
					second := f.addMachine(name+"-2", nil)
					f.machine.Name = name + "-1"
					cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}, Spec: clusterv1.ClusterSpec{ControlPlaneEndpoint: clusterv1.APIEndpoint{Host: "localhost", Port: 6443}}}
					require.NoError(t, c.Create(ctx, cluster))
					cluster.Status.Initialization.InfrastructureProvisioned = ptr.To(true)
					require.NoError(t, c.Status().Update(ctx, cluster))
					f.tcp.Finalizers = []string{controlplanev1.TalosControlPlaneFinalizer}
					f.tcp.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(cluster, clusterv1.GroupVersion.WithKind("Cluster"))}
					f.tcp.Spec.MachineTemplate.Spec.InfrastructureRef.Kind = "TestMachine"
					f.tcp.Spec.MachineTemplate.Spec.InfrastructureRef.Name = f.machine.Name
					machines := collections.FromMachines(f.machine, second)
					f.controlPlane.Machines = machines
					f.controlPlane.infraObjects = map[string]*unstructured.Unstructured{}
					f.controlPlane.talosConfigs = map[string]*cabptv1.TalosConfig{}
					for _, m := range machines {
						m.Spec.InfrastructureRef = clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.cluster.x-k8s.io", Kind: "TestMachine", Name: m.Name}
						m.Spec.Bootstrap.ConfigRef = clusterv1.ContractVersionedObjectReference{APIGroup: cabptv1.GroupVersion.Group, Kind: "TalosConfig", Name: m.Name}
						infra := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "infrastructure.cluster.x-k8s.io/v1beta2", "kind": "TestMachine", "metadata": map[string]any{"name": m.Name, "namespace": "default"}, "spec": map[string]any{"identity": m.Name}}}
						m.Spec.ClusterName = cluster.Name
						m.Labels = controlPlaneMachineLabelsForCluster(f.tcp, cluster.Name)
						m.Annotations = map[string]string{PreTerminateHookCleanupAnnotation: ""}
						ref := f.tcp.Spec.MachineTemplate.Spec.InfrastructureRef
						infra.SetAnnotations(map[string]string{clusterv1.TemplateClonedFromNameAnnotation: ref.Name, clusterv1.TemplateClonedFromGroupKindAnnotation: schema.GroupKind{Group: ref.APIGroup, Kind: ref.Kind}.String()})
						config := &cabptv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Name: m.Name, Namespace: "default"}, Spec: f.tcp.Spec.ControlPlaneConfig.ControlPlaneConfig}
						config.Spec.Hostname.Source = cabptv1.HostnameSourceMachineName
						require.NoError(t, c.Create(ctx, infra))
						require.NoError(t, c.Create(ctx, config))
						nodeRef := m.Status.NodeRef
						m.ResourceVersion = ""
						m.UID = ""
						require.NoError(t, c.Create(ctx, m))
						m.Status.NodeRef = nodeRef
						m.Status.NodeRef.Name = m.Name
						require.NoError(t, c.Status().Update(ctx, m))
						f.controlPlane.infraObjects[m.Name] = infra
						f.controlPlane.talosConfigs[m.Name] = config
					}
					f.tcp.Spec.ControlPlaneConfig.ControlPlaneConfig = f.controlPlane.talosConfigs[f.machine.Name].Spec
					f.tcp.Spec.ControlPlaneConfig.ControlPlaneConfig.ConfigRevision = "shared-2"
					f.tcp.Spec.ControlPlaneConfig.ControlPlaneConfig.StrategicPatchSecretRefs = []cabptv1.SecretKeyReference{{Name: "shared-private", Key: "patch"}}
					sharedSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "shared-private", Namespace: "default"}, Data: map[string][]byte{"patch": []byte("private-fixture")}}
					if err := c.Create(ctx, sharedSecret); err != nil {
						require.True(t, apierrors.IsAlreadyExists(err))
					}
					for _, m := range machines {
						f.tcp.Spec.MachineConfigOverrides = append(f.tcp.Spec.MachineConfigOverrides, controlplanev1.MachineConfigOverride{Name: m.Name, UID: m.UID, Config: &cabptv1.MachineConfigOverlay{ConfigSecretRef: &cabptv1.SecretKeyReference{Name: m.Name + "-baseline", Key: "value"}, ConfigRevision: "node-3", StrategicPatches: []string{"node-first", "node-second"}}, TalosEndpoint: &cabptv1.TalosEndpoint{Address: "unused-egress:50000", ServerName: m.Name + ".example.test", Node: m.Name}})
						require.NoError(t, c.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: m.Name + "-baseline", Namespace: "default"}, Data: map[string][]byte{"value": []byte("private-fixture")}}))
					}
					secondDesired, err := f.tcp.Spec.EffectiveTalosConfigSpec(second)
					require.NoError(t, err)
					f.controlPlane.talosConfigs[second.Name].Spec = *secondDesired
					require.NoError(t, c.Update(ctx, f.controlPlane.talosConfigs[second.Name]))
					require.NoError(t, c.Create(ctx, f.tcp))
					for _, m := range machines {
						m.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(f.tcp, controlplanev1.GroupVersion.WithKind("TalosControlPlane"))}
						require.NoError(t, c.Update(ctx, m))
					}
					var observedPods []*corev1.Pod
					for _, m := range machines {
						node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: m.Name, Labels: map[string]string{"node-role.kubernetes.io/control-plane": ""}}}
						require.NoError(t, c.Create(ctx, node))
						pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "kube-apiserver-" + m.Name, Namespace: "kube-system", Labels: map[string]string{"component": "kube-apiserver"}}, Spec: corev1.PodSpec{NodeName: m.Name, Containers: []corev1.Container{{Name: "kube-apiserver", Image: "registry/kube-apiserver:v1.34.0"}}}}
						require.NoError(t, c.Create(ctx, pod))
						pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "kube-apiserver", Image: "registry/kube-apiserver:v1.33.0", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}
						require.NoError(t, c.Status().Update(ctx, pod))
						observedPods = append(observedPods, pod)
					}
					f.reconciler.ClusterCache = clustercache.NewFakeClusterCache(c, client.ObjectKeyFromObject(cluster))
					f.reconciler.observeControlPlaneVersion(ctx, f.tcp, cluster, collections.ToMachineList(machines).Items)
					// Persist genuinely observed old version before the owner changes intent.
					require.Equal(t, "v1.33.0", *f.tcp.Status.Version)
					require.NoError(t, c.Status().Update(ctx, f.tcp))
					plan, err := f.reconciler.buildInPlacePlan(f.tcp, f.controlPlane, f.machine)
					require.NoError(t, err)
					require.NoError(t, f.reconciler.triggerInPlaceUpdate(ctx, f.tcp, cluster, plan))
					ownerOutput(t, f.reconciler, cluster, f.machine)
					ownerSecret, frozenPlan, err := f.reconciler.readOwnerPlan(ctx, f.tcp)
					require.NoError(t, err)
					_, err = f.reconciler.resumeOwnerPlan(ctx, f.tcp, cluster, ownerSecret, frozenPlan)
					require.NoError(t, err)
					require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(f.machine), f.machine))
					require.Equal(t, f.tcp.Spec.Version, f.machine.Spec.Version)
					require.True(t, isUpdatingInPlace(f.machine))
					if allMatched {
						second.Spec.Version = f.tcp.Spec.Version
						require.NoError(t, c.Update(ctx, second))
					}
					before := second.DeepCopy()
					var inventoryBefore clusterv1.MachineList
					require.NoError(t, c.List(ctx, &inventoryBefore))
					secondConfig := f.controlPlane.talosConfigs[second.Name].DeepCopy()
					secondInfra := f.controlPlane.infraObjects[second.Name].DeepCopy()
					observed, err := newControlPlane(ctx, c, &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Namespace: "default"}}, f.tcp, machines)
					require.NoError(t, err)
					firstDesired, err := f.tcp.Spec.EffectiveTalosConfigSpec(f.machine)
					require.NoError(t, err)
					require.Equal(t, *firstDesired, observed.talosConfigs[f.machine.Name].Spec)
					outdated, err := observed.MachinesWithOutdatedRolloutSpec()
					require.NoError(t, err)
					require.NotContains(t, outdated, f.machine.Name)
					if allMatched {
						require.Empty(t, outdated)
					}
					dialer := newFakeEtcdDialer()
					for _, m := range machines {
						dialer.clients[m.Name] = &fakeEtcdCalls{name: m.Name}
					}
					f.reconciler.etcdDialer = dialer.dial
					conditions.Set(f.tcp, metav1.Condition{Type: string(controlplanev1.EtcdClusterHealthyCondition), Status: metav1.ConditionTrue, Reason: controlplanev1.EtcdClusterHealthyReason})
					clusterCache := clustercache.NewFakeClusterCache(c, client.ObjectKeyFromObject(cluster))
					for _, pod := range observedPods {
						pod.Status.ContainerStatuses[0].Image = "registry/kube-apiserver:v1.34.0"
						require.NoError(t, c.Status().Update(ctx, pod))
					}
					for range 2 { // a restarted owner reads the same durable pending annotation
						f.reconciler = &TalosControlPlaneReconciler{Client: c, APIReader: c, Scheme: scheme, Log: ctrl.Log, RuntimeClient: f.caller, etcdDialer: dialer.dial, ClusterCache: clusterCache}
						list := collections.ToMachineList(machines)
						result, err := f.reconciler.reconcileMachines(ctx, cluster, f.tcp, &list)
						require.NoError(t, err)
						require.Positive(t, result.RequeueAfter)
						result, err = f.reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.tcp)})
						require.NoError(t, err)
						require.Positive(t, result.RequeueAfter)
						require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(f.machine), f.machine))
						require.True(t, conditions.IsFalse(f.machine, clusterv1.MachineUpToDateCondition), "spec-matched pending Machine must remain consumer-visible not UpToDate")
						require.Equal(t, clusterv1.MachineUpToDateUpdatingReason, conditions.Get(f.machine, clusterv1.MachineUpToDateCondition).Reason)
						require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(f.tcp), f.tcp))
						expectedUpToDate := int32(0)
						if allMatched {
							expectedUpToDate = 1
						}
						require.Equal(t, expectedUpToDate, *f.tcp.Status.UpToDateReplicas, "pending first Machine must not count toward control-plane update completion")
						require.Equal(t, "v1.33.0", *f.tcp.Status.Version, "new running image must not advance version while pending recovery")
						require.True(t, conditions.IsFalse(f.tcp, controlplanev1.VersionObservedCondition))
						require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(second), second))
						require.Equal(t, before.Spec, second.Spec)
						require.Equal(t, before.Annotations, second.Annotations, "no second Machine transaction or deletion")
						require.True(t, second.DeletionTimestamp.IsZero())
						require.Zero(t, f.caller.calls)
						var configs cabptv1.TalosConfig
						require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(secondConfig), &configs))
						require.Equal(t, secondConfig.ResourceVersion, configs.ResourceVersion, "no second bootstrap mutation")
						infra := secondInfra.DeepCopy()
						require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(infra), infra))
						require.Equal(t, secondInfra.GetResourceVersion(), infra.GetResourceVersion(), "no second infrastructure mutation")
						var all clusterv1.MachineList
						require.NoError(t, c.List(ctx, &all))
						require.Len(t, all.Items, len(inventoryBefore.Items), "no replacement or scale-up Machine")
					}
					ownerReceipt(t, &ownerFixture{r: f.reconciler, tcp: f.tcp, cluster: cluster, machine: f.machine, config: f.controlPlane.talosConfigs[f.machine.Name]}, ownerSecret, frozenPlan)
					delete(f.machine.Annotations, clusterv1.UpdateInProgressAnnotation)
					delete(f.machine.Annotations, runtimev1.PendingHooksAnnotation)
					require.NoError(t, c.Update(ctx, f.machine))
					for _, object := range []client.Object{f.controlPlane.talosConfigs[f.machine.Name], f.controlPlane.infraObjects[f.machine.Name]} {
						require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(object), object))
						annotations := object.GetAnnotations()
						delete(annotations, clusterv1.UpdateInProgressAnnotation)
						object.SetAnnotations(annotations)
						require.NoError(t, c.Update(ctx, object))
					}
					ownerSecret, frozenPlan, err = f.reconciler.readOwnerPlan(ctx, f.tcp)
					require.NoError(t, err)
					_, err = f.reconciler.resumeOwnerPlan(ctx, f.tcp, cluster, ownerSecret, frozenPlan)
					require.NoError(t, err)
					require.NoError(t, f.reconciler.updateStatus(ctx, f.tcp, cluster))
					require.Equal(t, "v1.34.0", *f.tcp.Status.Version)
					require.True(t, conditions.IsTrue(f.tcp, controlplanev1.VersionObservedCondition))
					completed := &clusterv1.Machine{}
					require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(f.machine), completed))
					require.True(t, conditions.IsTrue(completed, clusterv1.MachineUpToDateCondition))
					t.Logf("first=%s desired spec matches, hook pending; second=%s unchanged; gate=%t replicas=%d", f.machine.Name, second.Name, enabled, replicas)
				})
			}
		}
	}
}

func moduleDirectory(t *testing.T, module string) string {
	t.Helper()
	output, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", module).Output()
	require.NoError(t, err)
	return strings.TrimSpace(string(output))
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/collections"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	cp "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

func memberFixtureTCP(members ...cp.ControlPlaneMember) (*clusterv1.Cluster, *cp.TalosControlPlane) {
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "kissanpentu", Namespace: "cluster", UID: "cluster-uid"}, Spec: clusterv1.ClusterSpec{ControlPlaneEndpoint: clusterv1.APIEndpoint{Host: "localhost", Port: 6443}}}
	tcp := &cp.TalosControlPlane{
		ObjectMeta: metav1.ObjectMeta{Name: "kissanpentu", Namespace: "cluster", UID: "tcp-uid", Labels: map[string]string{clusterv1.ClusterNameLabel: cluster.Name}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cluster, clusterv1.GroupVersion.WithKind("Cluster"))}},
		Spec: cp.TalosControlPlaneSpec{
			Version:  "v1.35.0",
			Replicas: ptr.To[int32](int32(len(members))),
			Manage:   true,
			MachineTemplate: cp.TalosControlPlaneMachineTemplate{Spec: cp.TalosControlPlaneMachineTemplateSpec{
				InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.cluster.x-k8s.io", Kind: "TestMachineTemplate", Name: "kissanpentu-control-plane"},
			}},
			ControlPlaneConfig: cp.ControlPlaneConfig{
				ControlPlaneConfig: bootstrapv1.TalosConfigSpec{
					GenerateType:             "controlplane",
					TalosVersion:             "v1.14",
					StrategicPatches:         []string{"shared"},
					StrategicPatchSecretRefs: []bootstrapv1.SecretKeyReference{{Name: "kissanpentu-registry-auth", Key: "patch"}},
				},
				Members: members,
			},
		},
	}
	return cluster, tcp
}

func fixtureMember(name string) cp.ControlPlaneMember {
	return cp.ControlPlaneMember{
		Name:                   name,
		InfrastructureSelector: map[string]string{"caph.craci.dev/node": name},
		TalosConfigPatch: &cp.ControlPlaneMemberPatch{
			StrategicPatches:         []string{"patch-" + name},
			StrategicPatchSecretRefs: []bootstrapv1.SecretKeyReference{{Name: "kissanpentu-" + name + "-tailscale-patch", Key: "patch"}},
			TalosEndpoint:            &bootstrapv1.TalosEndpoint{Address: name + "-talos-access.cluster.svc.cluster.local:50000", ServerName: name + ".example.ts.net"},
		},
	}
}

// memberFixtureClient is a fake API with the infrastructure contract CRD and a template whose
// hostSelector already carries a role label, so the member selector has something to merge over.
func memberFixtureClient(t *testing.T, objects ...client.Object) (client.Client, *runtime.Scheme) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, cp.AddToScheme(scheme))
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	require.NoError(t, apiextensionsv1.AddToScheme(scheme))
	preserve := true
	crd := &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: "testmachinetemplates.infrastructure.cluster.x-k8s.io", Labels: map[string]string{"cluster.x-k8s.io/v1beta2": "v1beta2"}}, Spec: apiextensionsv1.CustomResourceDefinitionSpec{Group: "infrastructure.cluster.x-k8s.io", Scope: apiextensionsv1.NamespaceScoped, Names: apiextensionsv1.CustomResourceDefinitionNames{Plural: "testmachinetemplates", Singular: "testmachinetemplate", Kind: "TestMachineTemplate"}, Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{Name: "v1beta2", Served: true, Storage: true, Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: &preserve}}}}}}
	template := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "infrastructure.cluster.x-k8s.io/v1beta2", "kind": "TestMachineTemplate",
		"metadata": map[string]any{"name": "kissanpentu-control-plane", "namespace": "cluster"},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"hostSelector": map[string]any{"matchLabels": map[string]any{"caph.craci.dev/role": "control-plane"}},
			"installImage": map[string]any{"imageURLCommand": "install"},
		}}},
	}}
	api := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&clusterv1.Machine{}, &cp.TalosControlPlane{}, &bootstrapv1.TalosConfig{}).
		WithObjects(append([]client.Object{crd, template}, objects...)...).
		WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.CreateOption) error {
			if object.GetUID() == "" {
				object.SetUID(types.UID("fake-" + object.GetName()))
			}
			return c.Create(ctx, object, opts...)
		}}).Build()
	return api, scheme
}

func existingMemberMachine(tcp *cp.TalosControlPlane, member string, deleting bool) *clusterv1.Machine {
	m := &clusterv1.Machine{
		ObjectMeta: metav1.ObjectMeta{Name: member, Namespace: tcp.Namespace, UID: types.UID("machine-" + member),
			Labels:          map[string]string{clusterv1.ClusterNameLabel: "kissanpentu", clusterv1.MachineControlPlaneLabel: "", cp.MemberLabel: member},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(tcp, cp.GroupVersion.WithKind("TalosControlPlane"))}},
		Spec: clusterv1.MachineSpec{ClusterName: "kissanpentu", Version: "v1.35.0"},
	}
	if deleting {
		m.Finalizers = []string{"test"}
		m.DeletionTimestamp = ptr.To(metav1.Now())
	}
	return m
}

func TestBootControlPlane_CreatesTheNextFreeMember(t *testing.T) {
	cluster, tcp := memberFixtureTCP(fixtureMember("tiikerinpentu"), fixtureMember("leijonanpentu"), fixtureMember("jaguaarinpentu"))
	occupied := existingMemberMachine(tcp, "tiikerinpentu", false)
	api, scheme := memberFixtureClient(t, cluster, tcp, occupied)
	r := &TalosControlPlaneReconciler{Client: api, APIReader: api, Scheme: scheme, Log: ctrl.Log}

	res, err := r.bootControlPlane(context.Background(), cluster, tcp)
	require.NoError(t, err)
	require.Equal(t, 20*time.Second, res.RequeueAfter)

	machine := &clusterv1.Machine{}
	require.NoError(t, api.Get(context.Background(), client.ObjectKey{Namespace: "cluster", Name: "leijonanpentu"}, machine), "the first free member in declared order names the Machine")
	require.Equal(t, "leijonanpentu", machine.Labels[cp.MemberLabel])
	require.Equal(t, "", machine.Labels[clusterv1.MachineControlPlaneLabel])

	infra := &unstructured.Unstructured{}
	infra.SetAPIVersion("infrastructure.cluster.x-k8s.io/v1beta2")
	infra.SetKind("TestMachine")
	require.NoError(t, api.Get(context.Background(), client.ObjectKey{Namespace: "cluster", Name: "leijonanpentu"}, infra))
	selector, _, err := unstructured.NestedStringMap(infra.Object, "spec", "hostSelector", "matchLabels")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"caph.craci.dev/role": "control-plane", "caph.craci.dev/node": "leijonanpentu"}, selector, "member selector merges over the template's labels")
	require.Equal(t, "leijonanpentu", infra.GetLabels()[cp.MemberLabel])
	command, _, _ := unstructured.NestedString(infra.Object, "spec", "installImage", "imageURLCommand")
	require.Equal(t, "install", command, "the rest of the template spec is cloned untouched")

	config := &bootstrapv1.TalosConfig{}
	require.NoError(t, api.Get(context.Background(), client.ObjectKey{Namespace: "cluster", Name: "leijonanpentu"}, config))
	require.Equal(t, []string{"shared", "patch-leijonanpentu"}, config.Spec.StrategicPatches)
	require.Equal(t, "leijonanpentu-talos-access.cluster.svc.cluster.local:50000", config.Spec.TalosEndpoint.Address)
	require.True(t, config.Spec.Manage)
}

func TestBootControlPlane_WaitsForADeletingMemberMachine(t *testing.T) {
	cluster, tcp := memberFixtureTCP(fixtureMember("tiikerinpentu"))
	tcp.Spec.Replicas = ptr.To[int32](1)
	api, scheme := memberFixtureClient(t, cluster, tcp, existingMemberMachine(tcp, "tiikerinpentu", true))
	r := &TalosControlPlaneReconciler{Client: api, APIReader: api, Scheme: scheme, Log: ctrl.Log}

	res, err := r.bootControlPlane(context.Background(), cluster, tcp)
	require.NoError(t, err, "no free member is a wait, not a failure")
	require.Equal(t, 20*time.Second, res.RequeueAfter)
	list := &clusterv1.MachineList{}
	require.NoError(t, api.List(context.Background(), list, client.InNamespace("cluster")))
	require.Len(t, list.Items, 1, "the member name is reused only once the old Machine is gone")

	var condition *metav1.Condition
	for i := range tcp.Status.Conditions {
		if tcp.Status.Conditions[i].Type == string(cp.MachinesCreatedCondition) {
			condition = &tcp.Status.Conditions[i]
		}
	}
	require.NotNil(t, condition)
	require.Contains(t, condition.Message, "already have a Machine")
}

func TestBootControlPlane_WithoutMembersKeepsGeneratedNames(t *testing.T) {
	cluster, tcp := memberFixtureTCP()
	tcp.Spec.Replicas = ptr.To[int32](1)
	api, scheme := memberFixtureClient(t, cluster, tcp)
	r := &TalosControlPlaneReconciler{Client: api, APIReader: api, Scheme: scheme, Log: ctrl.Log}

	_, err := r.bootControlPlane(context.Background(), cluster, tcp)
	require.NoError(t, err)
	list := &clusterv1.MachineList{}
	require.NoError(t, api.List(context.Background(), list, client.InNamespace("cluster")))
	require.Len(t, list.Items, 1)
	require.Regexp(t, `^kissanpentu-[a-z0-9]{5}$`, list.Items[0].Name)
	require.NotContains(t, list.Items[0].Labels, cp.MemberLabel)
}

func TestSelectMachineForScaleDown_MembersLeaveInReverseDeclaredOrder(t *testing.T) {
	_, tcp := memberFixtureTCP(fixtureMember("tiikerinpentu"), fixtureMember("leijonanpentu"), fixtureMember("jaguaarinpentu"))
	first := existingMemberMachine(tcp, "tiikerinpentu", false)
	second := existingMemberMachine(tcp, "leijonanpentu", false)
	third := existingMemberMachine(tcp, "jaguaarinpentu", false)
	// Make the first-declared member the oldest so age-based selection would pick it.
	first.CreationTimestamp = metav1.NewTime(time.Now().Add(-3 * time.Hour))
	second.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Hour))
	third.CreationTimestamp = metav1.NewTime(time.Now().Add(-1 * time.Hour))
	plane := &ControlPlane{TCP: tcp, Machines: collections.FromMachines(first, second, third)}

	victim, err := selectMachineForScaleDown(plane, collections.New())
	require.NoError(t, err)
	require.Equal(t, "jaguaarinpentu", victim.Name, "highest declared index goes first")

	// An operator-marked Machine always wins.
	second.Annotations = map[string]string{clusterv1.DeleteMachineAnnotation: ""}
	victim, err = selectMachineForScaleDown(plane, collections.New())
	require.NoError(t, err)
	require.Equal(t, "leijonanpentu", victim.Name)

	// A Machine outside the declared membership is removed before any member.
	stray := existingMemberMachine(tcp, "stray", false)
	delete(second.Annotations, clusterv1.DeleteMachineAnnotation)
	plane.Machines = collections.FromMachines(first, second, third, stray)
	victim, err = selectMachineForScaleDown(plane, collections.New())
	require.NoError(t, err)
	require.Equal(t, "stray", victim.Name)
}

// The in-place planner must compare the live child against the member-resolved spec, so an
// up-to-date member does not look outdated merely because its overlay differs from the shared base.
func TestInPlaceDesiredSpecUsesMemberOverlay(t *testing.T) {
	cluster, tcp := memberFixtureTCP(fixtureMember("tiikerinpentu"), fixtureMember("leijonanpentu"))
	machine := existingMemberMachine(tcp, "leijonanpentu", false)
	machine.Spec.InfrastructureRef = clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.cluster.x-k8s.io", Kind: "TestMachine", Name: machine.Name}
	machine.Spec.Bootstrap.ConfigRef = clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: machine.Name}
	effective, err := tcp.Spec.EffectiveTalosConfigSpec(machine)
	require.NoError(t, err)
	config := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Name: machine.Name, Namespace: "cluster"}, Spec: *effective}
	infra := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "infrastructure.cluster.x-k8s.io/v1beta2", "kind": "TestMachine", "metadata": map[string]any{"name": machine.Name, "namespace": "cluster"}, "spec": map[string]any{}}}
	plane := &ControlPlane{TCP: tcp, Cluster: cluster, Machines: collections.FromMachines(machine), infraObjects: map[string]*unstructured.Unstructured{machine.Name: infra}, talosConfigs: map[string]*bootstrapv1.TalosConfig{machine.Name: config}}

	outdated, err := plane.MachinesWithOutdatedRolloutSpec()
	require.NoError(t, err)
	require.Equal(t, 0, outdated.Len(), "a child carrying its member overlay is up to date")

	r := &TalosControlPlaneReconciler{Log: ctrl.Log}
	plan, err := r.buildInPlacePlan(tcp, plane, machine)
	require.NoError(t, err)
	require.Equal(t, config.Spec, plan.desiredTalosConfig.Spec)

	tcp.Spec.ControlPlaneConfig.Members[1].TalosConfigPatch.StrategicPatches = []string{"patch-leijonanpentu", "rotated"}
	outdated, err = plane.MachinesWithOutdatedRolloutSpec()
	require.NoError(t, err)
	require.Equal(t, 1, outdated.Len(), "an overlay change on this member makes only this child outdated")
	plan, err = r.buildInPlacePlan(tcp, plane, machine)
	require.NoError(t, err)
	require.Equal(t, []string{"shared", "patch-leijonanpentu", "rotated"}, plan.desiredTalosConfig.Spec.StrategicPatches)
	require.Equal(t, config.Spec.TalosEndpoint, plan.desiredTalosConfig.Spec.TalosEndpoint, "the live child's endpoint is kept")
}

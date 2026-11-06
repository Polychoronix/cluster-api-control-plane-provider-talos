// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"testing"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimev1 "sigs.k8s.io/cluster-api/api/runtime/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func versionFixtures() ([]clusterv1.Machine, []corev1.Node, []corev1.Pod) {
	var machines []clusterv1.Machine
	var nodes []corev1.Node
	var pods []corev1.Pod
	for _, name := range []string{"cp-one", "cp-two"} {
		machines = append(machines, clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: clusterv1.MachineSpec{Version: "v1.36.0", ProviderID: "talos://" + name}, Status: clusterv1.MachineStatus{NodeRef: clusterv1.MachineNodeReference{Name: name}}})
		nodes = append(nodes, corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{"node-role.kubernetes.io/control-plane": ""}}, Spec: corev1.NodeSpec{ProviderID: "talos://" + name}})
		pods = append(pods, corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "kube-apiserver-" + name, Namespace: "kube-system", Labels: map[string]string{"component": "kube-apiserver"}}, Spec: corev1.PodSpec{NodeName: name, Containers: []corev1.Container{{Name: "kube-apiserver", Image: "registry.k8s.io/kube-apiserver:v1.36.0"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "kube-apiserver", Image: "registry.example:5000/kube-apiserver:v1.35.0@sha256:1234", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}})
	}
	return machines, nodes, pods
}

func TestMinimumObservedAPIServerVersion(t *testing.T) {
	machines, nodes, pods := versionFixtures()
	version, err := minimumObservedAPIServerVersion(machines, nodes, pods)
	require.NoError(t, err)
	require.Equal(t, "v1.35.0", version, "must observe running image, not desired Pod or Machine spec")
	for _, tc := range []struct {
		name   string
		change func([]clusterv1.Machine, []corev1.Node, []corev1.Pod) ([]clusterv1.Machine, []corev1.Node, []corev1.Pod)
	}{
		{"missing node", func(m []clusterv1.Machine, n []corev1.Node, p []corev1.Pod) ([]clusterv1.Machine, []corev1.Node, []corev1.Pod) {
			return m, n[:1], p
		}},
		{"duplicate node association", func(m []clusterv1.Machine, n []corev1.Node, p []corev1.Pod) ([]clusterv1.Machine, []corev1.Node, []corev1.Pod) {
			m[1].Status.NodeRef.Name = m[0].Status.NodeRef.Name
			return m, n, p
		}},
		{"mismatched provider identity", func(m []clusterv1.Machine, n []corev1.Node, p []corev1.Pod) ([]clusterv1.Machine, []corev1.Node, []corev1.Pod) {
			n[0].Spec.ProviderID = "replacement"
			return m, n, p
		}},
		{"missing pod", func(m []clusterv1.Machine, n []corev1.Node, p []corev1.Pod) ([]clusterv1.Machine, []corev1.Node, []corev1.Pod) {
			return m, n, p[:1]
		}},
		{"duplicate pod", func(m []clusterv1.Machine, n []corev1.Node, p []corev1.Pod) ([]clusterv1.Machine, []corev1.Node, []corev1.Pod) {
			return m, n, append(p, p[0])
		}},
		{"not running", func(m []clusterv1.Machine, n []corev1.Node, p []corev1.Pod) ([]clusterv1.Machine, []corev1.Node, []corev1.Pod) {
			p[0].Status.ContainerStatuses[0].State.Running = nil
			return m, n, p
		}},
		{"not ready", func(m []clusterv1.Machine, n []corev1.Node, p []corev1.Pod) ([]clusterv1.Machine, []corev1.Node, []corev1.Pod) {
			p[0].Status.ContainerStatuses[0].Ready = false
			return m, n, p
		}},
		{"missing container status", func(m []clusterv1.Machine, n []corev1.Node, p []corev1.Pod) ([]clusterv1.Machine, []corev1.Node, []corev1.Pod) {
			p[0].Status.ContainerStatuses = nil
			return m, n, p
		}},
		{"opaque image", func(m []clusterv1.Machine, n []corev1.Node, p []corev1.Pod) ([]clusterv1.Machine, []corev1.Node, []corev1.Pod) {
			p[0].Status.ContainerStatuses[0].Image = "registry/kube-apiserver@sha256:private"
			return m, n, p
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, n, p := versionFixtures()
			m, n, p = tc.change(m, n, p)
			_, err := minimumObservedAPIServerVersion(m, n, p)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "private")
		})
	}
}

func TestObservedVersionDoesNotAdvanceFromIntentOrPending(t *testing.T) {
	machines, nodes, pods := versionFixtures()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	var objects []client.Object
	for i := range nodes {
		objects = append(objects, &nodes[i], &pods[i])
	}
	api := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&corev1.Pod{}).WithObjects(objects...).Build()
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"}}
	r := &TalosControlPlaneReconciler{ClusterCache: clustercache.NewFakeClusterCache(api, client.ObjectKeyFromObject(cluster))}
	legacy := "v1.36.0"
	tcp := &controlplanev1.TalosControlPlane{Status: controlplanev1.TalosControlPlaneStatus{Version: &legacy}}
	r.observeControlPlaneVersion(t.Context(), tcp, cluster, machines)
	require.Equal(t, "v1.35.0", *tcp.Status.Version)
	require.True(t, conditions.IsTrue(tcp, controlplanev1.VersionObservedCondition))
	for i := range pods {
		pods[i].Status.ContainerStatuses[0].Image = "registry/kube-apiserver:v1.36.0"
		require.NoError(t, api.Status().Update(t.Context(), &pods[i]))
	}
	machines[1].Annotations = map[string]string{runtimev1.PendingHooksAnnotation: "UpdateMachine"}
	r.observeControlPlaneVersion(t.Context(), tcp, cluster, machines)
	require.Equal(t, "v1.35.0", *tcp.Status.Version)
	require.True(t, conditions.IsFalse(tcp, controlplanev1.VersionObservedCondition))
	machines[1].Annotations = nil
	r.observeControlPlaneVersion(t.Context(), tcp, cluster, machines)
	require.Equal(t, "v1.36.0", *tcp.Status.Version)
	require.True(t, conditions.IsTrue(tcp, controlplanev1.VersionObservedCondition))
	require.NoError(t, api.Delete(t.Context(), &nodes[1]))
	r.observeControlPlaneVersion(t.Context(), tcp, cluster, machines)
	require.Equal(t, "v1.36.0", *tcp.Status.Version, "temporary partial observation keeps prior genuine version")
	unverified := &controlplanev1.TalosControlPlane{Status: controlplanev1.TalosControlPlaneStatus{Version: &legacy}}
	r.observeControlPlaneVersion(t.Context(), unverified, cluster, machines)
	require.Nil(t, unverified.Status.Version, "unproven inherited intent is not retained")
}

type deniedVersionReader struct{ client.Client }

func (r deniedVersionReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return fmt.Errorf("denied private credentials must not be reflected")
}

func TestVersionObservationInitialDenialClearsUnverifiedIntent(t *testing.T) {
	machines, _, _ := versionFixtures()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	reader := deniedVersionReader{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"}}
	r := &TalosControlPlaneReconciler{ClusterCache: clustercache.NewFakeClusterCache(reader, client.ObjectKeyFromObject(cluster))}
	intent := "v1.36.0"
	tcp := &controlplanev1.TalosControlPlane{Status: controlplanev1.TalosControlPlaneStatus{Version: &intent}}
	r.observeControlPlaneVersion(t.Context(), tcp, cluster, machines)
	require.Nil(t, tcp.Status.Version)
	require.True(t, conditions.IsFalse(tcp, controlplanev1.VersionObservedCondition))
	require.NotContains(t, conditions.Get(tcp, controlplanev1.VersionObservedCondition).Message, "private")
	r.Client = reader
	legacy := &controlplanev1.TalosControlPlane{Status: controlplanev1.TalosControlPlaneStatus{Version: &intent}}
	require.Error(t, r.updateStatus(t.Context(), legacy, cluster))
	require.Nil(t, legacy.Status.Version, "even the first management lookup failure must clear inherited intent")
}

func TestObservedImageVersionTags(t *testing.T) {
	for _, image := range []string{"registry/kube-apiserver:v1.36.0", "registry:5000/kube-apiserver:v1.36.0@sha256:opaque"} {
		version, err := observedImageVersion(image)
		require.NoError(t, err)
		require.Equal(t, "1.36.0", version.String())
	}
	for _, image := range []string{"registry/kube-apiserver@sha256:opaque", "registry:5000/kube-apiserver", "registry/kube-apiserver:latest"} {
		_, err := observedImageVersion(image)
		require.Error(t, err)
	}
}

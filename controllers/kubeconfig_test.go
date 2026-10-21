// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"strings"
	"testing"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const externalAuthProxyKubeconfig = `apiVersion: v1
kind: Config
current-context: proxy
contexts:
- name: proxy
  context:
    cluster: proxy
    user: proxy
clusters:
- name: proxy
  cluster:
    server: https://auth-proxy.example:443
users:
- name: proxy
  user:
    token: private-sentinel
`

func TestExternalKubeconfig_NeverWritesOrRotates(t *testing.T) {
	for _, test := range []struct {
		name    string
		data    []byte
		label   string
		missing bool
		valid   bool
	}{
		{"auth-proxy", []byte(externalAuthProxyKubeconfig), "kissanpentu", false, true},
		{"invalid", []byte("private-sentinel: ["), "kissanpentu", false, false},
		{"empty", nil, "kissanpentu", false, false},
		{"wrong-label", []byte(externalAuthProxyKubeconfig), "other", false, false},
		{"missing", nil, "", true, false},
		{"exec", []byte(strings.Replace(externalAuthProxyKubeconfig, "token: private-sentinel", "exec:\n      apiVersion: client.authentication.k8s.io/v1\n      command: never-execute\n      interactiveMode: Never", 1)), "kissanpentu", false, false},
		{"auth-provider", []byte(strings.Replace(externalAuthProxyKubeconfig, "token: private-sentinel", "auth-provider:\n      name: oidc", 1)), "kissanpentu", false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			scheme := runtime.NewScheme()
			require.NoError(t, corev1.AddToScheme(scheme))
			s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "kissanpentu-kubeconfig", Namespace: "workload", Labels: map[string]string{clusterv1.ClusterNameLabel: test.label}, Annotations: map[string]string{"managed-by": "user"}}, Data: map[string][]byte{"value": test.data}}
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if !test.missing {
				builder.WithObjects(s)
			}
			c := builder.Build()
			r := &TalosControlPlaneReconciler{Client: c, APIReader: c, Log: ctrl.Log}
			cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "kissanpentu", Namespace: "workload"}}
			tcp := &controlplanev1.TalosControlPlane{Spec: controlplanev1.TalosControlPlaneSpec{Kubeconfig: controlplanev1.KubeconfigSpec{Mode: controlplanev1.KubeconfigModeExternal}}}
			var before corev1.Secret
			if !test.missing {
				require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(s), &before))
			}
			for range 2 {
				_, err := r.reconcileKubeconfig(ctx, cluster, tcp, nil)
				if test.valid {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					require.NotContains(t, err.Error(), "private-sentinel")
				}
				require.Equal(t, test.valid, conditions.IsTrue(tcp, controlplanev1.KubeconfigAvailableCondition))
				if !test.missing {
					var after corev1.Secret
					require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(s), &after))
					require.Equal(t, before, after, "external credentials, metadata and ownership remain unchanged")
				} else {
					var secrets corev1.SecretList
					require.NoError(t, c.List(ctx, &secrets))
					require.Empty(t, secrets.Items, "external mode never creates a replacement Secret")
				}
			}
		})
	}
}

func TestExternalKubeconfig_ObservedAPIAvailability(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	s := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "kissanpentu-kubeconfig", Namespace: "workload", Labels: map[string]string{clusterv1.ClusterNameLabel: "kissanpentu"}}, Data: map[string][]byte{"value": []byte(externalAuthProxyKubeconfig)}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(s).Build()
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "kissanpentu", Namespace: "workload"}}
	tcp := &controlplanev1.TalosControlPlane{Spec: controlplanev1.TalosControlPlaneSpec{Kubeconfig: controlplanev1.KubeconfigSpec{Mode: controlplanev1.KubeconfigModeExternal}}}
	r := &TalosControlPlaneReconciler{Client: c, APIReader: c, ClusterCache: clustercache.NewFakeClusterCache(c, client.ObjectKeyFromObject(cluster)), Log: ctrl.Log}
	require.NoError(t, r.updateStatus(ctx, tcp, cluster))
	require.True(t, conditions.IsTrue(tcp, string(controlplanev1.AvailableCondition)), "successful workload API Node list is an observation")
	r.ClusterCache = clustercache.NewFakeClusterCache(c, client.ObjectKey{Name: "different", Namespace: "workload"})
	require.NoError(t, r.updateStatus(ctx, tcp, cluster))
	require.False(t, conditions.IsTrue(tcp, string(controlplanev1.AvailableCondition)), "unreachable cache must not retain stale availability")
}

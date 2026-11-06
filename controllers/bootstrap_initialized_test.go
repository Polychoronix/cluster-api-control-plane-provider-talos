// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"github.com/go-logr/logr"
	cp "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"testing"
)

func TestInitializedClusterNeverIssuesTalosBootstrap(t *testing.T) {
	for _, initialization := range []bool{true, false} {
		t.Run(map[bool]string{true: "initialization", false: "condition"}[initialization], func(t *testing.T) {
			cluster := &clusterv1.Cluster{}
			if initialization {
				cluster.Status.Initialization.ControlPlaneInitialized = ptr.To(true)
			} else {
				conditions.Set(cluster, metav1.Condition{Type: "ControlPlaneInitialized", Status: metav1.ConditionTrue, Reason: "Observed"})
			}
			tcp := &cp.TalosControlPlane{}
			r := &TalosControlPlaneReconciler{Log: logr.Discard()}
			// No client/identity exists: reaching any LS/Bootstrap transport would fail or panic.
			require.NoError(t, r.bootstrapCluster(t.Context(), cluster, tcp, []clusterv1.Machine{{ObjectMeta: metav1.ObjectMeta{Name: "empty-reachable-candidate"}}}))
			require.True(t, tcp.Status.Bootstrapped)
		})
	}
}

// Every sticky proof that the control plane has existed refuses Bootstrap before any
// Talos RPC: a Machine with a NodeRef, more than one Machine, the TalosControlPlane's own
// initialization or bootstrapped status, and the Cluster's deprecated v1beta1 condition.
func TestInitializedEvidenceNeverIssuesTalosBootstrap(t *testing.T) {
	joined := clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "kissanpentu-g8fjk"}}
	joined.Status.NodeRef = clusterv1.MachineNodeReference{Name: "tiikerinpentu"}
	fresh := clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "leijonanpentu"}}
	deprecated := &clusterv1.Cluster{}
	deprecated.Status.Deprecated = &clusterv1.ClusterDeprecatedStatus{V1Beta1: &clusterv1.ClusterV1Beta1DeprecatedStatus{Conditions: clusterv1.Conditions{{Type: "ControlPlaneInitialized", Status: corev1.ConditionTrue}}}}
	initializedTCP := &cp.TalosControlPlane{}
	initializedTCP.Status.Initialization.ControlPlaneInitialized = ptr.To(true)
	bootstrappedTCP := &cp.TalosControlPlane{}
	bootstrappedTCP.Status.Bootstrapped = true
	for name, c := range map[string]struct {
		cluster  *clusterv1.Cluster
		tcp      *cp.TalosControlPlane
		machines []clusterv1.Machine
		evidence string
	}{
		"machine with NodeRef":     {&clusterv1.Cluster{}, &cp.TalosControlPlane{}, []clusterv1.Machine{joined}, "Machine kissanpentu-g8fjk has a NodeRef"},
		"wiped member plus joined": {&clusterv1.Cluster{}, &cp.TalosControlPlane{}, []clusterv1.Machine{{ObjectMeta: metav1.ObjectMeta{Name: "a"}}, fresh}, "2 control-plane Machines exist"},
		"deprecated condition":     {deprecated, &cp.TalosControlPlane{}, []clusterv1.Machine{fresh}, "Cluster deprecated v1beta1 condition ControlPlaneInitialized"},
		"tcp initialized":          {&clusterv1.Cluster{}, initializedTCP, []clusterv1.Machine{fresh}, "TalosControlPlane status.initialization.controlPlaneInitialized"},
		"tcp bootstrapped":         {&clusterv1.Cluster{}, bootstrappedTCP, []clusterv1.Machine{fresh}, "TalosControlPlane status.bootstrapped"},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, c.evidence, clusterInitializedEvidence(c.cluster, c.tcp, c.machines))
			r := &TalosControlPlaneReconciler{Log: logr.Discard()}
			// No client/identity exists: reaching any LS/Bootstrap transport would fail or panic.
			require.NoError(t, r.bootstrapCluster(t.Context(), c.cluster, c.tcp, c.machines))
			require.True(t, c.tcp.Status.Bootstrapped)
		})
	}
	require.Equal(t, "", clusterInitializedEvidence(&clusterv1.Cluster{}, &cp.TalosControlPlane{}, []clusterv1.Machine{fresh}))
}

// Without evidence, a node whose state cannot be read refuses Bootstrap instead of
// defaulting to it (no identity Secret here, so the Talos client cannot be built).
func TestUnprovableNodeRefusesTalosBootstrap(t *testing.T) {
	r := &TalosControlPlaneReconciler{Log: logr.Discard()}
	tcp := &cp.TalosControlPlane{}
	machine := clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "fresh", Namespace: "default"}}
	err := r.bootstrapCluster(t.Context(), &clusterv1.Cluster{}, tcp, []clusterv1.Machine{machine})
	require.ErrorContains(t, err, "refusing Talos Bootstrap")
	require.False(t, tcp.Status.Bootstrapped)
}

func TestFreshClusterStillRequiresBootstrapCandidate(t *testing.T) {
	r := &TalosControlPlaneReconciler{Log: logr.Discard()}
	tcp := &cp.TalosControlPlane{}
	require.ErrorContains(t, r.bootstrapCluster(t.Context(), &clusterv1.Cluster{}, tcp, nil), "no Machines")
	require.False(t, tcp.Status.Bootstrapped)
}

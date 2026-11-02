// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"github.com/go-logr/logr"
	cp "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
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
func TestFreshClusterStillRequiresBootstrapCandidate(t *testing.T) {
	r := &TalosControlPlaneReconciler{Log: logr.Discard()}
	tcp := &cp.TalosControlPlane{}
	require.ErrorContains(t, r.bootstrapCluster(t.Context(), &clusterv1.Cluster{}, tcp, nil), "no Machines")
	require.False(t, tcp.Status.Bootstrapped)
}

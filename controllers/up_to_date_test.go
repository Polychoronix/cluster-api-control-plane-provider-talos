// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimev1 "sigs.k8s.io/cluster-api/api/runtime/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestMachineUpToDateWaitsForInPlaceObservation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(*clusterv1.Machine)
	}{
		{"pending", func(machine *clusterv1.Machine) {
			machine.Annotations = map[string]string{runtimev1.PendingHooksAnnotation: "UpdateMachine"}
		}},
		{"inprogress", func(machine *clusterv1.Machine) {
			machine.Annotations = map[string]string{clusterv1.UpdateInProgressAnnotation: ""}
		}},
		{"core updating", func(machine *clusterv1.Machine) {
			conditions.Set(machine, metav1.Condition{Type: clusterv1.MachineUpdatingCondition, Status: metav1.ConditionTrue, Reason: clusterv1.MachineInPlaceUpdatingReason})
		}},
		{"observed kubelet old", func(machine *clusterv1.Machine) {
			machine.Status.NodeInfo = &corev1.NodeSystemInfo{KubeletVersion: "v1.33.0"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newInPlaceFixture(t, nil)
			f.machine.Spec.Version = f.tcp.Spec.Version
			tc.prepare(f.machine)
			c := fake.NewClientBuilder().WithScheme(f.reconciler.Scheme).WithStatusSubresource(&clusterv1.Machine{}).WithObjects(f.machine).Build()
			require.NoError(t, f.controlPlane.reconcileMachineUpToDateConditions(t.Context(), c))
			condition := conditions.Get(f.machine, clusterv1.MachineUpToDateCondition)
			require.NotNil(t, condition)
			require.Equal(t, metav1.ConditionFalse, condition.Status)
			require.Equal(t, clusterv1.MachineUpToDateUpdatingReason, condition.Reason)
			// Core's completed handshake plus matching observed kubelet permits
			// progression here; durable receipt gating is a separate pending slice.
			f.machine.Annotations = nil
			conditions.Set(f.machine, metav1.Condition{Type: clusterv1.MachineUpdatingCondition, Status: metav1.ConditionFalse, Reason: clusterv1.MachineNotUpdatingReason})
			f.machine.Status.NodeInfo = &corev1.NodeSystemInfo{KubeletVersion: f.machine.Spec.Version}
			require.NoError(t, f.controlPlane.reconcileMachineUpToDateConditions(t.Context(), c))
			require.True(t, conditions.IsTrue(f.machine, clusterv1.MachineUpToDateCondition))
		})
	}
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"testing"

	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestMachineConfigOverridePlanAndRolloutUseSameSpec(t *testing.T) {
	f := newInPlaceFixture(t, nil)
	f.machine.UID = "machine-one"
	shared := &f.tcp.Spec.ControlPlaneConfig.ControlPlaneConfig
	shared.ConfigRevision = "shared-2"
	shared.StrategicPatches = []string{"first", "second"}
	shared.StrategicPatchSecretRefs = []cabptv1.SecretKeyReference{{Name: "shared-private", Key: "patch"}}
	f.tcp.Spec.MachineConfigOverrides = []controlplanev1.MachineConfigOverride{{Name: f.machine.Name, UID: f.machine.UID, Config: &cabptv1.MachineConfigOverlay{ConfigRevision: "node-3", ConfigSecretRef: &cabptv1.SecretKeyReference{Name: "node-base", Key: "value"}, StrategicPatches: []string{"node-first", "node-second"}}}}
	plan, err := f.reconciler.buildInPlacePlan(f.tcp, f.controlPlane, f.machine)
	require.NoError(t, err)
	desired, err := f.tcp.Spec.EffectiveTalosConfigSpec(f.machine)
	require.NoError(t, err)
	require.Equal(t, *desired, plan.desiredTalosConfig.Spec)
	require.Equal(t, []string{"first", "second"}, plan.desiredTalosConfig.Spec.StrategicPatches)
	require.Equal(t, "shared-2", plan.desiredTalosConfig.Spec.ConfigRevision)
	require.Equal(t, "node-3", plan.desiredTalosConfig.Spec.MachineConfig.ConfigRevision)
	f.machine.Spec.Version = f.tcp.Spec.Version
	f.controlPlane.talosConfigs[f.machine.Name].Spec = *desired
	outdated, err := f.controlPlane.MachinesWithOutdatedRolloutSpec()
	require.NoError(t, err)
	require.Empty(t, outdated)
	f.tcp.Spec.MachineConfigOverrides[0].Config.ConfigRevision = "node-4"
	outdated, err = f.controlPlane.MachinesWithOutdatedRolloutSpec()
	require.NoError(t, err)
	require.Contains(t, outdated, f.machine.Name)
	f.tcp.Spec.MachineConfigOverrides[0].UID = "replaced"
	_, err = f.controlPlane.MachinesWithOutdatedRolloutSpec()
	require.ErrorContains(t, err, "UID pin")
	_, err = f.reconciler.buildInPlacePlan(f.tcp, f.controlPlane, f.machine)
	require.ErrorContains(t, err, "UID pin")
}

func TestMachineConfigOverrideRejectsBeforeEarlyPhases(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(map[bool]string{false: "stale UID", true: "shared overlay conflict"}[conflict], func(t *testing.T) {
			f := newInPlaceFixture(t, nil)
			f.machine.Labels = map[string]string{clusterv1.ClusterNameLabel: "cluster", clusterv1.MachineControlPlaneLabel: ""}
			require.NoError(t, f.reconciler.Client.Update(t.Context(), f.machine))
			f.tcp.Spec.MachineConfigOverrides = []controlplanev1.MachineConfigOverride{{Name: f.machine.Name, UID: "different", Config: &cabptv1.MachineConfigOverlay{ConfigRevision: "node"}}}
			if conflict {
				f.tcp.Spec.ControlPlaneConfig.ControlPlaneConfig.MachineConfig = &cabptv1.MachineConfigOverlay{}
			}
			var calls int
			f.reconciler.etcdDialer = func(_ context.Context, _ *controlplanev1.TalosControlPlane, _ ...clusterv1.Machine) (etcdCalls, error) {
				calls++
				return nil, nil
			}
			before := f.machine.DeepCopy()
			_, err := f.reconciler.reconcile(t.Context(), &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"}}, f.tcp)
			require.Error(t, err)
			require.Zero(t, calls)
			stored := &clusterv1.Machine{}
			require.NoError(t, f.reconciler.Client.Get(t.Context(), client.ObjectKeyFromObject(f.machine), stored))
			require.Equal(t, before.Annotations, stored.Annotations)
			require.Equal(t, before.Spec, stored.Spec)
			cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"}}
			cluster.Status.Initialization.InfrastructureProvisioned = ptr.To(true)
			f.tcp.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(cluster, clusterv1.GroupVersion.WithKind("Cluster"))}
			f.tcp.Finalizers = []string{controlplanev1.TalosControlPlaneFinalizer}
			deleting := metav1.Now()
			f.tcp.DeletionTimestamp = &deleting
			f.reconciler.Client = fake.NewClientBuilder().WithScheme(f.reconciler.Scheme).WithObjects(cluster, f.tcp, f.machine).Build()
			f.reconciler.APIReader = f.reconciler.Client
			_, err = f.reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.tcp)})
			require.Error(t, err, "invalid pins must block TCP deletion before its Machine mutations")
			require.NoError(t, f.reconciler.Client.Get(t.Context(), client.ObjectKeyFromObject(f.machine), stored))
			require.True(t, stored.DeletionTimestamp.IsZero())
			require.Equal(t, before.Annotations, stored.Annotations)
		})
	}
}

func TestMachineConfigOverrideLivePinCannotHideOutsideSelector(t *testing.T) {
	f := newInPlaceFixture(t, nil)
	f.tcp.Spec.MachineConfigOverrides = []controlplanev1.MachineConfigOverride{{Name: f.machine.Name, UID: "expected", Config: &cabptv1.MachineConfigOverlay{ConfigRevision: "node"}}}
	// Cached inventory has no matching Machine. The uncached reader sees the
	// reused name even without CP labels, so it cannot be treated as inert.
	f.reconciler.APIReader = f.reconciler.Client
	f.reconciler.Client = fake.NewClientBuilder().WithScheme(f.reconciler.Scheme).Build()
	require.ErrorContains(t, f.reconciler.validateLiveMachineConfigPins(t.Context(), f.tcp), "UID pin")
	f.tcp.Spec.MachineConfigOverrides[0].Name = "not-associated-yet"
	require.NoError(t, f.reconciler.validateLiveMachineConfigPins(t.Context(), f.tcp))
	f.reconciler.APIReader = nil
	require.ErrorContains(t, f.reconciler.validateLiveMachineConfigPins(t.Context(), f.tcp), "APIReader is required")
	_, _, err := f.reconciler.talosClientForMachine(t.Context(), f.tcp, f.machine)
	require.ErrorContains(t, err, "APIReader is required")
}

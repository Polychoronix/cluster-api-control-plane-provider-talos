// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"testing"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestLifecycleMachineSelectionRequiresExactTCPController(t *testing.T) {
	f := newOwnerFixture(t, nil)
	for _, tc := range []struct {
		name   string
		mutate func(*metav1.OwnerReference)
	}{
		{"other UID", func(o *metav1.OwnerReference) { o.UID = "recreated-owner" }},
		{"other name", func(o *metav1.OwnerReference) { o.Name = "another-tcp" }},
		{"other group", func(o *metav1.OwnerReference) { o.APIVersion = "foreign.example/v1beta1" }},
		{"other kind", func(o *metav1.OwnerReference) { o.Kind = "MachineSet" }},
		{"noncontroller", func(o *metav1.OwnerReference) { no := false; o.Controller = &no }},
	} {
		m := f.machine.DeepCopy()
		m.UID = types.UID(tc.name)
		m.ResourceVersion = ""
		m.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(f.tcp, controlplanev1.GroupVersion.WithKind("TalosControlPlane"))}
		tc.mutate(&m.OwnerReferences[0])
		t.Run(tc.name, func(t *testing.T) {
			m.Name = "candidate"
			require.NoError(t, f.r.Client.Create(t.Context(), m))
			before := m.DeepCopy()
			owned, err := f.r.getControlPlaneMachinesForTCP(t.Context(), client.ObjectKeyFromObject(f.cluster), f.tcp)
			require.NoError(t, err)
			require.Len(t, owned.Items, 1)
			require.Equal(t, f.machine.UID, owned.Items[0].UID)
			inventory, err := f.r.listControlPlaneMachinesForCluster(t.Context(), client.ObjectKeyFromObject(f.cluster))
			require.NoError(t, err)
			require.Len(t, inventory.Items, 2, "read-only discovery must still see unassociated inventory")
			require.NoError(t, f.r.Client.Get(t.Context(), client.ObjectKeyFromObject(m), m))
			require.Equal(t, before, m, "selection does not take over a foreign Machine")
			require.NoError(t, f.r.Client.Delete(t.Context(), m))
		})
	}
}

func TestTCPDeletionCannotMutateForeignLabelledMachine(t *testing.T) {
	f := newOwnerFixture(t, nil)
	foreign := f.machine.DeepCopy()
	foreign.Name, foreign.UID, foreign.ResourceVersion = "foreign", "foreign-uid", ""
	foreign.OwnerReferences = nil
	foreign.Annotations = map[string]string{PreTerminateHookCleanupAnnotation: ""}
	require.NoError(t, f.r.Client.Create(t.Context(), foreign))
	before := foreign.DeepCopy()
	_, err := f.r.reconcileDelete(t.Context(), f.cluster, f.tcp)
	require.NoError(t, err)
	require.NoError(t, f.r.Client.Get(t.Context(), client.ObjectKeyFromObject(foreign), foreign))
	require.Equal(t, before.Spec, foreign.Spec)
	require.Equal(t, before.OwnerReferences, foreign.OwnerReferences)
	require.Equal(t, before.Annotations, foreign.Annotations)
	require.True(t, foreign.DeletionTimestamp.IsZero())
}

func TestEtcdAuditDoesNotTreatUnassociatedMachineAsOrphan(t *testing.T) {
	f := newOwnerFixture(t, nil)
	foreign := f.machine.DeepCopy()
	foreign.Name, foreign.UID, foreign.ResourceVersion = "unassociated", "foreign-uid", ""
	foreign.OwnerReferences = nil
	require.NoError(t, f.r.Client.Create(t.Context(), foreign))
	// There is no Talos identity fixture. If audit proceeds beyond inventory
	// rejection it would open a client: this test requires the exact earlier gate.
	err := f.r.auditEtcd(t.Context(), f.tcp, client.ObjectKeyFromObject(f.cluster))
	require.ErrorContains(t, err, "not controlled by this TalosControlPlane")
}

func TestDeclaredObservationInventoryDoesNotNeedTCPAssociation(t *testing.T) {
	f := newOwnerFixture(t, nil)
	f.machine.OwnerReferences = nil
	require.NoError(t, f.r.Client.Update(t.Context(), f.machine))
	f.infra.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(f.machine, clusterv1.GroupVersion.WithKind("Machine"))})
	require.NoError(t, f.r.Client.Update(t.Context(), f.infra))
	beforeMachine, beforeConfig, beforeInfra := f.machine.DeepCopy(), f.config.DeepCopy(), f.infra.DeepCopy()
	list, err := f.r.readDeclaredControlPlaneInventory(t.Context(), f.cluster, []*clusterv1.Machine{f.machine})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	require.Empty(t, list.Items[0].OwnerReferences, "observation is not activation")
	for _, object := range []client.Object{f.machine, f.config, f.infra} {
		require.NoError(t, f.r.Client.Get(t.Context(), client.ObjectKeyFromObject(object), object))
	}
	require.Equal(t, beforeMachine, f.machine)
	require.Equal(t, beforeConfig, f.config)
	require.Equal(t, beforeInfra, f.infra)
	for _, paused := range []client.Object{f.machine, f.config} {
		paused.SetAnnotations(map[string]string{clusterv1.PausedAnnotation: ""})
		require.NoError(t, f.r.Client.Update(t.Context(), paused))
		_, err := f.r.readDeclaredControlPlaneInventory(t.Context(), f.cluster, []*clusterv1.Machine{f.machine})
		require.ErrorContains(t, err, "paused")
		paused.SetAnnotations(nil)
		require.NoError(t, f.r.Client.Update(t.Context(), paused))
	}
	f.config.OwnerReferences[0].UID = "another-machine"
	require.NoError(t, f.r.Client.Update(t.Context(), f.config))
	_, err = f.r.readDeclaredControlPlaneInventory(t.Context(), f.cluster, []*clusterv1.Machine{f.machine})
	require.ErrorContains(t, err, "ownership")
}

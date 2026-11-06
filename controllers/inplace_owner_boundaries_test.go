// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"context"
	"fmt"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	cp "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimev1 "sigs.k8s.io/cluster-api/api/runtime/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

func TestOwnerReleasedResponseLossNeverRestamps(t *testing.T) {
	for _, boundary := range []int{1, 2} {
		t.Run(fmt.Sprint(boundary), func(t *testing.T) {
			f := newOwnerFixture(t, nil)
			require.NoError(t, f.r.triggerInPlaceUpdate(t.Context(), f.tcp, f.cluster, f.update))
			ownerOutput(t, f.r, f.cluster, f.machine)
			s, p, e := f.r.readOwnerPlan(t.Context(), f.tcp)
			require.NoError(t, e)
			api := f.r.Client
			f.r.Client = &faultOwnerClient{Client: api, after: boundary}
			held, e := f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
			require.True(t, held)
			require.Error(t, e)
			f.r.Client = api
			s, p, e = f.r.readOwnerPlan(t.Context(), f.tcp)
			require.NoError(t, e)
			require.Equal(t, op.OwnerReleased, p.Intent.Phase)
			require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.machine), f.machine))
			before := f.machine.DeepCopy()
			held, e = f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
			require.True(t, held)
			if boundary == 1 {
				require.Error(t, e)
			} else {
				require.NoError(t, e)
			}
			require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.machine), f.machine))
			require.Equal(t, before.ResourceVersion, f.machine.ResourceVersion)
			require.Equal(t, before.Annotations, f.machine.Annotations)
		})
	}
}

func TestOwnerReceiptPartialCleanupCannotRelease(t *testing.T) {
	f := releasedOwner(t)
	s, p, e := f.r.readOwnerPlan(t.Context(), f.tcp)
	require.NoError(t, e)
	ownerReceipt(t, f, s, p)
	require.NoError(t, f.r.Client.Get(t.Context(), client.ObjectKeyFromObject(f.machine), f.machine))
	delete(f.machine.Annotations, clusterv1.UpdateInProgressAnnotation)
	require.NoError(t, f.r.Client.Update(t.Context(), f.machine))
	held, e := f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
	require.True(t, held)
	require.NoError(t, e)
	j, _, e := op.ReadJournal(t.Context(), f.r.APIReader, "default", sIdentity(f.config))
	require.NoError(t, e)
	require.Empty(t, j.Data[op.ConsumedReceiptKey])
	cleanupCoreFlags(t, f)
	held, e = f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
	require.NoError(t, e)
	require.False(t, held)
}

type lostOwnerDelete struct {
	client.Client
	fail bool
}

func (c *lostOwnerDelete) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	e := c.Client.Delete(ctx, obj, opts...)
	if e == nil && c.fail {
		c.fail = false
		return fmt.Errorf("lost delete response")
	}
	return e
}
func TestOwnerConsumedPlanDeletionResponseLoss(t *testing.T) {
	f := releasedOwner(t)
	s, p, e := f.r.readOwnerPlan(t.Context(), f.tcp)
	require.NoError(t, e)
	ownerReceipt(t, f, s, p)
	cleanupCoreFlags(t, f)
	api := f.r.Client
	f.r.Client = &lostOwnerDelete{Client: api, fail: true}
	held, e := f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
	require.True(t, held)
	require.Error(t, e)
	f.r.Client = api
	held, e = f.r.ownerUpdateHeld(t.Context(), f.tcp)
	require.NoError(t, e)
	require.False(t, held)
	j, _, e := op.ReadJournal(t.Context(), api, "default", sIdentity(f.config))
	require.NoError(t, e)
	require.NotEmpty(t, j.Data[op.ConsumedReceiptKey])
}

func TestOwnerActivePlanBlocksSecondDeletingMachine(t *testing.T) {
	f := releasedOwner(t)
	second := f.machine.DeepCopy()
	second.Name = "deleting-second"
	second.UID = "second-uid"
	second.ResourceVersion = ""
	second.Annotations = map[string]string{PreTerminateHookCleanupAnnotation: ""}
	second.Finalizers = []string{"fixture"}
	require.NoError(t, f.r.Client.Create(t.Context(), second))
	require.NoError(t, f.r.Client.Delete(t.Context(), second))
	f.r.ClusterCache = clustercache.NewFakeClusterCache(f.r.Client, client.ObjectKeyFromObject(f.cluster))
	etcd := &fakeEtcdCalls{serviceList: healthyServices()}
	f.r.etcdDialer = func(context.Context, *cp.TalosControlPlane, ...clusterv1.Machine) (etcdCalls, error) {
		return etcd, nil
	}
	f.cluster.Status.Initialization.InfrastructureProvisioned = new(bool)
	*f.cluster.Status.Initialization.InfrastructureProvisioned = true
	require.NoError(t, f.r.Client.Status().Update(t.Context(), f.cluster))
	_, e := f.r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.tcp)})
	require.Error(t, e)
	require.Zero(t, etcd.leaveCalls)
	require.Empty(t, etcd.removedIDs)
	require.Zero(t, etcd.forfeitCalls)
	require.NoError(t, f.r.Client.Get(t.Context(), client.ObjectKeyFromObject(second), second))
	require.Contains(t, second.Annotations, PreTerminateHookCleanupAnnotation)
}

func TestOwnerFrozenSourceRotationAfterRenderingStops(t *testing.T) {
	f := newOwnerFixture(t, nil)
	require.NoError(t, f.r.triggerInPlaceUpdate(t.Context(), f.tcp, f.cluster, f.update))
	ownerOutput(t, f.r, f.cluster, f.machine)
	source := &corev1.Secret{}
	require.NoError(t, f.r.Client.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "owner-baseline"}, source))
	source.Data["value"] = []byte("changed-private")
	require.NoError(t, f.r.Client.Update(t.Context(), source))
	s, p, e := f.r.readOwnerPlan(t.Context(), f.tcp)
	require.NoError(t, e)
	held, e := f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
	require.True(t, held)
	require.Error(t, e)
	require.NotContains(t, e.Error(), "changed-private")
	require.NoError(t, f.r.Client.Get(t.Context(), client.ObjectKeyFromObject(f.machine), f.machine))
	require.Empty(t, f.machine.Annotations[runtimev1.PendingHooksAnnotation])
}

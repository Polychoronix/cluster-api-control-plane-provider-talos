// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

func TestOwnerStaleCachedOwnerIntentCannotAuthorizeWrites(t *testing.T) {
	for _, owner := range []string{"tcp", "cluster"} {
		t.Run(owner, func(t *testing.T) {
			f := newOwnerFixture(t, nil)
			s, p, e := f.r.createOwnerPlan(t.Context(), f.tcp, f.cluster, f.update)
			require.NoError(t, e)
			if owner == "tcp" {
				live := f.tcp.DeepCopy()
				live.Spec.Version = "v1.35.0"
				require.NoError(t, f.r.Client.Update(t.Context(), live))
			} else {
				live := f.cluster.DeepCopy()
				live.Spec.ControlPlaneEndpoint.Host = "changed.example"
				require.NoError(t, f.r.Client.Update(t.Context(), live))
			}
			before := f.machine.DeepCopy()
			held, e := f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
			require.True(t, held)
			require.ErrorContains(t, e, "live frozen owner intent changed")
			require.NoError(t, f.r.Client.Get(t.Context(), client.ObjectKeyFromObject(f.machine), f.machine))
			require.Equal(t, before.Spec, f.machine.Spec)
			require.Equal(t, before.Annotations, f.machine.Annotations)
		})
	}
}
func TestOwnerPreparedRejectsConcurrentUnapprovedInfra(t *testing.T) {
	for _, change := range []string{"spec", "owner"} {
		t.Run(change, func(t *testing.T) {
			f := newOwnerFixture(t, nil)
			live := f.infra.DeepCopy()
			if change == "spec" {
				require.NoError(t, unstructured.SetNestedField(live.Object, "changed-unapproved", "spec", "identity"))
			} else {
				live.SetOwnerReferences(f.machine.OwnerReferences)
			}
			require.NoError(t, f.r.Client.Update(t.Context(), live))
			_, _, e := f.r.createOwnerPlan(t.Context(), f.tcp, f.cluster, f.update)
			require.ErrorContains(t, e, "CAN-approved infrastructure changed")
			_, p, e := f.r.readOwnerPlan(t.Context(), f.tcp)
			require.NoError(t, e)
			require.Nil(t, p)
		})
	}
}

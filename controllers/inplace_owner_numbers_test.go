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

func TestOwnerPlanPreservesExactJSONNumbersAcrossRestartAndRelease(t *testing.T) {
	f := newOwnerFixture(t, nil)
	require.NoError(t, unstructured.SetNestedField(f.infra.Object, int64(9007199254740993), "spec", "largeInteger"))
	require.NoError(t, unstructured.SetNestedField(f.infra.Object, int64(7), "spec", "defaultInteger"))
	require.NoError(t, unstructured.SetNestedField(f.infra.Object, float64(0.125), "spec", "fraction"))
	require.NoError(t, f.r.Client.Update(t.Context(), f.infra))
	f.update.desiredInfraMachine = f.infra.DeepCopy()
	s, p, e := f.r.createOwnerPlan(t.Context(), f.tcp, f.cluster, f.update)
	require.NoError(t, e)
	_, p, e = f.r.readOwnerPlan(t.Context(), f.tcp)
	require.NoError(t, e)
	require.Equal(t, int64(9007199254740993), p.InfrastructureSpec["largeInteger"])
	require.Equal(t, int64(7), p.InfrastructureSpec["defaultInteger"])
	require.Equal(t, float64(0.125), p.InfrastructureSpec["fraction"])
	_, e = f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
	require.NoError(t, e)
	ownerOutput(t, f.r, f.cluster, f.machine)
	s, p, e = f.r.readOwnerPlan(t.Context(), f.tcp)
	require.NoError(t, e)
	_, e = f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
	require.NoError(t, e)
	_, p, e = f.r.readOwnerPlan(t.Context(), f.tcp)
	require.NoError(t, e)
	require.Equal(t, int64(9007199254740993), p.InfrastructureSpec["largeInteger"])
	require.NoError(t, f.r.Client.Get(t.Context(), client.ObjectKeyFromObject(f.infra), f.infra))
	live, _, e := unstructured.NestedMap(f.infra.Object, "spec")
	require.NoError(t, e)
	require.Equal(t, p.InfrastructureSpec, live)
}

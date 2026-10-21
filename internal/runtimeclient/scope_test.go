// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package runtimeclient

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooksv1 "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	runtimev1 "sigs.k8s.io/cluster-api/api/runtime/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func scopeFixture(t *testing.T, selector *metav1.LabelSelector) (client.Client, *runtimev1.ExtensionConfig, *clusterv1.Machine) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, runtimev1.AddToScheme(scheme))
	config := &runtimev1.ExtensionConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "talos"},
		Spec:       runtimev1.ExtensionConfigSpec{NamespaceSelector: selector},
		Status: runtimev1.ExtensionConfigStatus{Handlers: []runtimev1.ExtensionHandler{{
			Name: "can-update.talos", RequestHook: runtimev1.GroupVersionHook{APIVersion: runtimehooksv1.GroupVersion.String(), Hook: "CanUpdateMachine"}, FailurePolicy: runtimev1.FailurePolicyFail,
		}}},
	}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "selected", Labels: map[string]string{"tenant": "selected"}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(config).WithObjects(config, namespace).Build()
	return c, config, &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "cp", Namespace: namespace.Name}}
}

func TestRuntimeScopeDiscovery(t *testing.T) {
	for _, tc := range []struct {
		name      string
		selector  *metav1.LabelSelector
		namespace string
		wantMatch bool
		wantError bool
	}{
		{name: "matching labels", selector: &metav1.LabelSelector{MatchLabels: map[string]string{"tenant": "selected"}}, wantMatch: true},
		{name: "excluded labels", selector: &metav1.LabelSelector{MatchLabels: map[string]string{"tenant": "other"}}},
		{name: "matching expressions", selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "tenant", Operator: metav1.LabelSelectorOpIn, Values: []string{"selected"}}}}, wantMatch: true},
		{name: "excluded expressions", selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "tenant", Operator: metav1.LabelSelectorOpNotIn, Values: []string{"selected"}}}}},
		{name: "empty selects all without namespace read", selector: &metav1.LabelSelector{}, namespace: "absent", wantMatch: true},
		{name: "missing namespace", selector: &metav1.LabelSelector{MatchLabels: map[string]string{"tenant": "selected"}}, namespace: "absent", wantError: true},
		{name: "invalid selector", selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "tenant", Operator: "invalid"}}}, wantError: true},
		{name: "missing selector was not admission defaulted", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _, machine := scopeFixture(t, tc.selector)
			if tc.namespace != "" {
				machine.Namespace = tc.namespace
			}
			names, err := New(c).GetAllExtensions(t.Context(), runtimehooksv1.CanUpdateMachine, machine)
			if tc.wantError {
				require.Error(t, err)
				require.Empty(t, names)
				require.Error(t, New(c).CallExtension(t.Context(), runtimehooksv1.CanUpdateMachine, machine, "can-update.talos", &runtimehooksv1.CanUpdateMachineRequest{}, &runtimehooksv1.CanUpdateMachineResponse{}))
				return
			}
			require.NoError(t, err)
			if tc.wantMatch {
				require.Equal(t, []string{"can-update.talos"}, names)
			} else {
				require.Empty(t, names)
			}
		})
	}
}

type failingScopeReader struct {
	client.Reader
	failList bool
}

func (r failingScopeReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return apierrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, "selected", fmt.Errorf("denied"))
}
func (r failingScopeReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if r.failList {
		return fmt.Errorf("ExtensionConfig list unavailable")
	}
	return r.Reader.List(ctx, list, opts...)
}

func TestRuntimeScopeReadFailures(t *testing.T) {
	for _, failList := range []bool{false, true} {
		t.Run(fmt.Sprintf("list=%t", failList), func(t *testing.T) {
			c, _, machine := scopeFixture(t, &metav1.LabelSelector{MatchLabels: map[string]string{"tenant": "selected"}})
			caller := New(failingScopeReader{Reader: c, failList: failList})
			names, err := caller.GetAllExtensions(t.Context(), runtimehooksv1.CanUpdateMachine, machine)
			require.Error(t, err)
			require.Empty(t, names)
			require.Error(t, caller.CallExtension(t.Context(), runtimehooksv1.CanUpdateMachine, machine, "can-update.talos", &runtimehooksv1.CanUpdateMachineRequest{}, &runtimehooksv1.CanUpdateMachineResponse{}))
		})
	}
}

func TestRuntimeScopeCallRechecksAndMergesSettings(t *testing.T) {
	var calls atomic.Int32
	observed := make(chan *runtimehooksv1.CanUpdateMachineRequest, 1)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var request runtimehooksv1.CanUpdateMachineRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		observed <- &request
		_ = json.NewEncoder(w).Encode(&runtimehooksv1.CanUpdateMachineResponse{CommonResponse: runtimehooksv1.CommonResponse{Status: runtimehooksv1.ResponseStatusSuccess}})
	}))
	t.Cleanup(server.Close)
	c, config, machine := scopeFixture(t, &metav1.LabelSelector{MatchLabels: map[string]string{"tenant": "selected"}})
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(config), config))
	config.Spec.ClientConfig = runtimev1.ClientConfig{URL: server.URL, CABundle: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})}
	config.Spec.Settings = map[string]string{"registration": "kept", "precedence": "registration"}
	require.NoError(t, c.Update(t.Context(), config))
	caller := New(c)
	names, err := caller.GetAllExtensions(t.Context(), runtimehooksv1.CanUpdateMachine, machine)
	require.NoError(t, err)
	require.Len(t, names, 1)
	request := &runtimehooksv1.CanUpdateMachineRequest{CommonRequest: runtimehooksv1.CommonRequest{Settings: map[string]string{"request": "kept", "precedence": "request"}}}
	original := request.DeepCopy()
	require.NoError(t, caller.CallExtension(t.Context(), runtimehooksv1.CanUpdateMachine, machine, names[0], request, &runtimehooksv1.CanUpdateMachineResponse{}))
	require.Equal(t, map[string]string{"registration": "kept", "request": "kept", "precedence": "request"}, (<-observed).Settings)
	require.Equal(t, original, request)
	require.EqualValues(t, 1, calls.Load())

	config.Spec.NamespaceSelector.MatchLabels["tenant"] = "other"
	require.NoError(t, c.Update(t.Context(), config))
	require.Error(t, caller.CallExtension(t.Context(), runtimehooksv1.CanUpdateMachine, machine, names[0], request, &runtimehooksv1.CanUpdateMachineResponse{}))
	require.EqualValues(t, 1, calls.Load(), "narrowing after discovery must not send another request")
	require.Equal(t, map[string]string{"registration": "kept", "precedence": "registration"}, config.Spec.Settings)
}

func TestRuntimeScopeCallRejectsWrongTypes(t *testing.T) {
	c, _, machine := scopeFixture(t, &metav1.LabelSelector{})
	caller := New(c)
	require.ErrorContains(t, caller.CallExtension(t.Context(), runtimehooksv1.CanUpdateMachine, machine, "can-update.talos", &runtimehooksv1.UpdateMachineRequest{}, &runtimehooksv1.CanUpdateMachineResponse{}), "request")
	require.ErrorContains(t, caller.CallExtension(t.Context(), runtimehooksv1.CanUpdateMachine, machine, "can-update.talos", &runtimehooksv1.CanUpdateMachineRequest{}, &runtimehooksv1.UpdateMachineResponse{}), "response")
	config := &runtimev1.ExtensionConfig{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Name: "talos"}, config))
	config.Status.Handlers[0].FailurePolicy = runtimev1.FailurePolicyIgnore
	require.NoError(t, c.Status().Update(t.Context(), config))
	require.ErrorContains(t, caller.CallExtension(t.Context(), runtimehooksv1.CanUpdateMachine, machine, "can-update.talos", &runtimehooksv1.CanUpdateMachineRequest{}, &runtimehooksv1.CanUpdateMachineResponse{}), "failurePolicy Fail")
}

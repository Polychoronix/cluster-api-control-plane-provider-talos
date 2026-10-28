// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package runtimeclient

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimecatalog "sigs.k8s.io/cluster-api/api/runtime/catalog"
	runtimehooksv1 "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	runtimev1 "sigs.k8s.io/cluster-api/api/runtime/v1beta2"
	runtimeserver "sigs.k8s.io/cluster-api/exp/runtime/server"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// This tests the CACPPT caller with a real CAPI Runtime SDK HTTPS server and
// Discovery-derived registration in an isolated management API server. It does
// not start the CABPT executor or core Machine/ExtensionConfig reconcilers.
func TestRuntimeScopeTLSEnvtest(t *testing.T) {
	ctx := t.Context()
	module, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "sigs.k8s.io/cluster-api").Output()
	require.NoError(t, err)
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join(strings.TrimSpace(string(module)), "core", "config", "crd", "bases", "runtime.cluster.x-k8s.io_extensionconfigs.yaml")}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil && os.Getenv("KUBEBUILDER_ASSETS") == "" && strings.Contains(err.Error(), "fork/exec") {
		t.Skipf("envtest binaries are not installed: %v", err)
	}
	require.NoError(t, err, "set KUBEBUILDER_ASSETS to local setup-envtest assets")
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, runtimev1.AddToScheme(scheme))
	api, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)

	certificateServer := httptest.NewTLSServer(http.NotFoundHandler())
	certificate := certificateServer.TLS.Certificates[0]
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateServer.Certificate().Raw})
	certificateServer.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	catalog := runtimecatalog.New()
	require.NoError(t, runtimehooksv1.AddToCatalog(catalog))
	server, err := runtimeserver.New(runtimeserver.Options{Catalog: catalog, Host: "127.0.0.1", Port: port, TLSOpts: []func(*tls.Config){func(config *tls.Config) {
		config.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return &certificate, nil }
	}}})
	require.NoError(t, err)
	var calls atomic.Int32
	observed := make(chan *runtimehooksv1.CanUpdateMachineRequest, 2)
	failPolicy := runtimehooksv1.FailurePolicyFail
	timeout := int32(10)
	require.NoError(t, server.AddExtensionHandler(runtimeserver.ExtensionHandler{Hook: runtimehooksv1.CanUpdateMachine, Name: "can-update", FailurePolicy: &failPolicy, TimeoutSeconds: &timeout, HandlerFunc: func(_ context.Context, request *runtimehooksv1.CanUpdateMachineRequest, response *runtimehooksv1.CanUpdateMachineResponse) {
		calls.Add(1)
		observed <- request.DeepCopy()
		response.Status = runtimehooksv1.ResponseStatusSuccess
		patch, err := json.Marshal(map[string]any{"spec": map[string]any{"version": request.Desired.Machine.Spec.Version}})
		if err != nil {
			response.Status = runtimehooksv1.ResponseStatusFailure
			return
		}
		response.MachinePatch = runtimehooksv1.Patch{PatchType: runtimehooksv1.JSONMergePatchType, Patch: patch}
	}}))
	serverCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- server.Start(serverCtx) }()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })
	require.Eventually(t, func() bool { return server.StartedChecker()(nil) == nil }, 10*time.Second, 10*time.Millisecond)
	config := &runtimev1.ExtensionConfig{ObjectMeta: metav1.ObjectMeta{Name: "scope-smoke"}, Spec: runtimev1.ExtensionConfigSpec{
		ClientConfig:      runtimev1.ClientConfig{URL: fmt.Sprintf("https://127.0.0.1:%d", port), CABundle: ca},
		NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"tenant": "selected"}},
		Settings:          map[string]string{"registration": "kept", "precedence": "registration"},
	}}
	// Perform actual TLS Discovery before storing its returned handler registration.
	httpClient, err := clientFor(config)
	require.NoError(t, err)
	discoveryGVH := gvhFor(t, runtimehooksv1.Discovery)
	discoveryEndpoint, err := endpointFor(config, &runtimev1.ExtensionHandler{}, discoveryGVH)
	require.NoError(t, err)
	discoveryBody, err := json.Marshal(&runtimehooksv1.DiscoveryRequest{})
	require.NoError(t, err)
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, discoveryEndpoint, bytes.NewReader(discoveryBody))
	require.NoError(t, err)
	httpResponse, err := httpClient.Do(httpRequest)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, httpResponse.StatusCode)
	var discovery runtimehooksv1.DiscoveryResponse
	err = json.NewDecoder(httpResponse.Body).Decode(&discovery)
	require.NoError(t, httpResponse.Body.Close())
	require.NoError(t, err)
	require.Equal(t, runtimehooksv1.ResponseStatusSuccess, discovery.Status)
	require.Len(t, discovery.Handlers, 1)
	handler := discovery.Handlers[0]
	require.Equal(t, "can-update", handler.Name)
	require.NotNil(t, handler.TimeoutSeconds)
	require.NotNil(t, handler.FailurePolicy)
	require.Equal(t, runtimehooksv1.FailurePolicyFail, *handler.FailurePolicy)
	require.NoError(t, api.Create(ctx, config))
	config.Status.Handlers = []runtimev1.ExtensionHandler{{Name: handler.Name + "." + config.Name, RequestHook: runtimev1.GroupVersionHook{APIVersion: handler.RequestHook.APIVersion, Hook: handler.RequestHook.Hook}, TimeoutSeconds: *handler.TimeoutSeconds, FailurePolicy: runtimev1.FailurePolicy(*handler.FailurePolicy)}}
	require.NoError(t, api.Status().Update(ctx, config))
	selected := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "selected", Labels: map[string]string{"tenant": "selected"}}}
	excluded := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "excluded", Labels: map[string]string{"tenant": "other"}}}
	require.NoError(t, api.Create(ctx, selected))
	require.NoError(t, api.Create(ctx, excluded))
	machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "cp", Namespace: selected.Name}}
	caller := New(api)
	names, err := caller.GetAllExtensions(ctx, runtimehooksv1.CanUpdateMachine, machine)
	require.NoError(t, err)
	require.Equal(t, []string{"can-update.scope-smoke"}, names)
	request := &runtimehooksv1.CanUpdateMachineRequest{CommonRequest: runtimehooksv1.CommonRequest{Settings: map[string]string{"precedence": "request"}}}
	request.Current.Machine = *machine
	request.Current.Machine.Spec.Version = "v1.35.0"
	request.Desired.Machine = *machine
	request.Desired.Machine.Spec.Version = "v1.36.0"
	original := request.DeepCopy()
	call := func(object *clusterv1.Machine) error {
		response := &runtimehooksv1.CanUpdateMachineResponse{}
		if err := caller.CallExtension(ctx, runtimehooksv1.CanUpdateMachine, object, names[0], request, response); err != nil {
			return err
		}
		require.Equal(t, runtimehooksv1.ResponseStatusSuccess, response.Status)
		require.Equal(t, runtimehooksv1.JSONMergePatchType, response.MachinePatch.PatchType)
		require.JSONEq(t, `{"spec":{"version":"v1.36.0"}}`, string(response.MachinePatch.Patch))
		return nil
	}
	require.NoError(t, call(machine))
	got := <-observed
	require.Equal(t, map[string]string{"registration": "kept", "precedence": "request"}, got.Settings)
	require.Equal(t, "v1.35.0", got.Current.Machine.Spec.Version)
	require.Equal(t, "v1.36.0", got.Desired.Machine.Spec.Version)
	require.Equal(t, original, request)
	require.EqualValues(t, 1, calls.Load())
	excludedMachine := machine.DeepCopy()
	excludedMachine.Namespace = excluded.Name
	excludedNames, err := caller.GetAllExtensions(ctx, runtimehooksv1.CanUpdateMachine, excludedMachine)
	require.NoError(t, err)
	require.Empty(t, excludedNames)
	require.Error(t, call(excludedMachine))
	require.EqualValues(t, 1, calls.Load(), "excluded namespace must not reach the TLS handler")

	// A name selected earlier must be fenced by namespace relabeling and by a
	// narrowed ExtensionConfig, even without another discovery call.
	selected.Labels["tenant"] = "other"
	require.NoError(t, api.Update(ctx, selected))
	require.Error(t, call(machine))
	relabeledNames, err := caller.GetAllExtensions(ctx, runtimehooksv1.CanUpdateMachine, machine)
	require.NoError(t, err)
	require.Empty(t, relabeledNames)
	require.EqualValues(t, 1, calls.Load())
	selected.Labels["tenant"] = "selected"
	require.NoError(t, api.Update(ctx, selected))
	config.Spec.NamespaceSelector = &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: corev1.LabelMetadataName, Operator: metav1.LabelSelectorOpIn, Values: []string{excluded.Name}}}}
	require.NoError(t, api.Update(ctx, config))
	require.Error(t, call(machine))
	narrowedNames, err := caller.GetAllExtensions(ctx, runtimehooksv1.CanUpdateMachine, machine)
	require.NoError(t, err)
	require.Empty(t, narrowedNames)
	require.EqualValues(t, 1, calls.Load())
	config.Spec.NamespaceSelector.MatchExpressions[0].Values = []string{selected.Name}
	require.NoError(t, api.Update(ctx, config))
	require.NoError(t, call(machine))
	<-observed
	require.EqualValues(t, 2, calls.Load(), "matching scope recovery must invoke the real handler")
	stored := &runtimev1.ExtensionConfig{}
	require.NoError(t, api.Get(ctx, client.ObjectKeyFromObject(config), stored))
	require.Equal(t, config.Spec.Settings, stored.Spec.Settings)
	require.Equal(t, original, request)
	t.Logf("real TLS Discovery and %d CanUpdateMachine calls; excluded, relabeled and narrowed scope emitted zero requests; matching scope recovered", calls.Load())
}

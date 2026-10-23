// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	cp "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestOwnerTransactionEnvtest(t *testing.T) {
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "config", "crd", "bases"), filepath.Join(moduleDirectory(t, "sigs.k8s.io/cluster-api"), "core", "config", "crd", "bases"), filepath.Join(moduleDirectory(t, "github.com/siderolabs/cluster-api-bootstrap-provider-talos"), "config", "crd", "bases")}, ErrorIfCRDPathMissing: true, CRDs: []*apiextensionsv1.CustomResourceDefinition{{ObjectMeta: metav1.ObjectMeta{Name: "testmachines.infrastructure.cluster.x-k8s.io", Labels: map[string]string{"cluster.x-k8s.io/v1beta2": "v1beta2"}}, Spec: apiextensionsv1.CustomResourceDefinitionSpec{Group: "infrastructure.cluster.x-k8s.io", Scope: apiextensionsv1.NamespaceScoped, Names: apiextensionsv1.CustomResourceDefinitionNames{Plural: "testmachines", Singular: "testmachine", Kind: "TestMachine", ListKind: "TestMachineList"}, Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{Name: "v1beta2", Served: true, Storage: true, Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", Properties: map[string]apiextensionsv1.JSONSchemaProps{"spec": {Type: "object", XPreserveUnknownFields: ptr.To(true)}}}}}}}}}}
	cfg, err := env.Start()
	if err != nil && os.Getenv("KUBEBUILDER_ASSETS") == "" && strings.Contains(err.Error(), "fork/exec") {
		t.Skipf("envtest binaries not installed: %v", err)
	}
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, cp.AddToScheme(scheme))
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	api, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	f := newOwnerFixture(t, api)
	// Two independent owners create concurrently. Atomic create must choose one
	// transaction, not overwrite one actor's frozen plan with the other's intent.
	var wg sync.WaitGroup
	ids := make(chan string, 2)
	errors := make(chan error, 2)
	for range 2 {
		wg.Go(func() {
			s, p, e := f.r.createOwnerPlan(context.Background(), f.tcp, f.cluster, f.update)
			if e == nil {
				ids <- p.Intent.TransactionID
				require.NotEmpty(t, s.UID)
			}
			errors <- e
		})
	}
	wg.Wait()
	close(ids)
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	var first string
	for id := range ids {
		if first == "" {
			first = id
		} else {
			require.Equal(t, first, id)
		}
	}
	s, p, err := f.r.readOwnerPlan(t.Context(), f.tcp)
	require.NoError(t, err)
	// Response loss after a real no-create write resumes frozen state.
	f.r.Client = &faultOwnerClient{Client: api, after: 2}
	held, err := f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
	require.True(t, held)
	require.Error(t, err)
	f.r.Client = api
	s, p, err = f.r.readOwnerPlan(t.Context(), f.tcp)
	require.NoError(t, err)
	held, err = f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
	require.NoError(t, err)
	require.True(t, held)
	// A stale RV write fails without trampling unrelated metadata.
	stale := f.machine.DeepCopy()
	require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(stale), stale))
	live := stale.DeepCopy()
	live.Labels["unrelated"] = "preserved"
	require.NoError(t, api.Update(t.Context(), live))
	require.Error(t, f.r.ownerPatch(t.Context(), stale, map[string]any{"op": "replace", "path": "/spec/version", "value": "v1.99.0"}))
	require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(stale), stale))
	require.Equal(t, "v1.34.0", stale.Spec.Version)
	ownerOutput(t, f.r, f.cluster, f.machine)
	s, p, err = f.r.readOwnerPlan(t.Context(), f.tcp)
	require.NoError(t, err)
	held, err = f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
	require.NoError(t, err)
	require.True(t, held)
	s, p, err = f.r.readOwnerPlan(t.Context(), f.tcp)
	require.NoError(t, err)
	require.Equal(t, op.OwnerReleased, p.Intent.Phase)
	// Missing receipt with flags absent is intervention, not restamping.
	cleanupCoreFlags(t, f)
	held, err = f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
	require.True(t, held)
	require.Error(t, err)
	ownerReceipt(t, f, s, p)
	f.r.Client = &faultOwnerClient{Client: api, after: 2}
	held, err = f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
	require.True(t, held)
	require.Error(t, err)
	f.r.Client = api
	s, p, err = f.r.readOwnerPlan(t.Context(), f.tcp)
	require.NoError(t, err)
	require.Equal(t, op.OwnerConsumed, p.Intent.Phase)
	held, err = f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
	require.NoError(t, err)
	require.False(t, held)
	_, p, err = f.r.readOwnerPlan(t.Context(), f.tcp)
	require.NoError(t, err)
	require.Nil(t, p)
	journal, _, err := op.ReadJournal(t.Context(), api, f.tcp.Namespace, sIdentity(f.config))
	require.NoError(t, err)
	require.NotEmpty(t, journal.Data[op.ConsumedReceiptKey])
	require.NotEmpty(t, journal.Data[op.ReceiptKey])
	// Fresh production creation path supplies initial-origin markers only on Create.
	ref, err := f.r.generateTalosConfig(t.Context(), f.tcp, "fresh-bootstrap", &bootstrapv1.TalosConfigSpec{GenerateType: "controlplane"})
	require.NoError(t, err)
	fresh := &bootstrapv1.TalosConfig{}
	require.NoError(t, api.Get(t.Context(), client.ObjectKey{Namespace: f.tcp.Namespace, Name: ref.Name}, fresh))
	require.Equal(t, f.tcp.Name, fresh.Annotations[op.InitialControlPlaneNameAnnotation])
	require.Equal(t, string(f.tcp.UID), fresh.Annotations[op.InitialControlPlaneUIDAnnotation])
	require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.config), f.config))
	require.Empty(t, f.config.Annotations[op.InitialControlPlaneNameAnnotation], "existing config never retroactively receives initial provenance")
	t.Log("actual API owner Create race, lost patch response, stale RV CAS, Released ambiguity, receipt consumption/Consumed cleanup and fresh TC provenance exercised; executor receipts fixture-produced")
}

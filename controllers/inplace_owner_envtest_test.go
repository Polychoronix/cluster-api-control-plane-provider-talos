// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
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
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"
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
	exerciseRetiringOwnerCleanup(t, api)
	exerciseNativeDeletionUIDFence(t, api)
	exerciseNativeReleaseIntentFence(t, api)
}

type retiringOwnerFaultClient struct {
	client.Client
	detach, remove bool
}

func (c *retiringOwnerFaultClient) Patch(ctx context.Context, object client.Object, p client.Patch, opts ...client.PatchOption) error {
	err := c.Client.Patch(ctx, object, p, opts...)
	if _, ok := object.(*bootstrapv1.TalosConfig); ok && c.detach && err == nil {
		c.detach = false
		return fmt.Errorf("lost real detach acknowledgment")
	}
	return err
}
func (c *retiringOwnerFaultClient) Delete(ctx context.Context, object client.Object, opts ...client.DeleteOption) error {
	err := c.Client.Delete(ctx, object, opts...)
	if _, ok := object.(*corev1.Secret); ok && c.remove && err == nil {
		c.remove = false
		return fmt.Errorf("lost real owner Secret delete acknowledgment")
	}
	return err
}

// This tests real API recovery of a fully witnessed fixture-produced executor
// receipt. It does not claim actual Talos execution or adoption activation.
func exerciseRetiringOwnerCleanup(t *testing.T, api client.Client) {
	for _, boundary := range []string{"detach", "delete", "active"} {
		t.Run("retirement-"+boundary, func(t *testing.T) {
			f := newOwnerFixture(t, api, "retire-owner-"+boundary)
			if boundary == "active" {
				_, _, err := f.r.createOwnerPlan(t.Context(), f.tcp, f.cluster, f.update)
				require.NoError(t, err)
			} else {
				require.NoError(t, f.r.triggerInPlaceUpdate(t.Context(), f.tcp, f.cluster, f.update))
				ownerOutput(t, f.r, f.cluster, f.machine)
				s, p, err := f.r.readOwnerPlan(t.Context(), f.tcp)
				require.NoError(t, err)
				_, err = f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
				require.NoError(t, err)
				s, p, err = f.r.readOwnerPlan(t.Context(), f.tcp)
				require.NoError(t, err)
				require.Equal(t, op.OwnerReleased, p.Intent.Phase)
				cleanupCoreFlags(t, f)
				ownerReceipt(t, f, s, p)
				f.r.Client = &faultOwnerClient{Client: api, after: 2}
				_, err = f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
				require.Error(t, err)
				f.r.Client = api
				_, p, err = f.r.readOwnerPlan(t.Context(), f.tcp)
				require.NoError(t, err)
				require.Equal(t, op.OwnerConsumed, p.Intent.Phase)
			}
			require.NoError(t, api.Delete(t.Context(), f.tcp))
			require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.tcp), f.tcp))
			require.False(t, f.tcp.DeletionTimestamp.IsZero())
			_, p, err := f.r.readOwnerPlan(t.Context(), f.tcp)
			require.NoError(t, err)
			request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.tcp)}
			if boundary == "active" {
				beforeMachine, beforeConfig, beforeInfra := f.machine.DeepCopy(), f.config.DeepCopy(), f.infra.DeepCopy()
				_, err = f.r.Reconcile(t.Context(), request)
				require.ErrorContains(t, err, "unfinished owner transaction")
				require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.machine), f.machine))
				require.Equal(t, beforeMachine, f.machine)
				require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.config), f.config))
				require.Equal(t, beforeConfig, f.config)
				require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.infra), f.infra))
				require.Equal(t, beforeInfra, f.infra)
				return
			}
			// New desired intent cannot reinterpret an old Consumed plan, even deleting.
			frozen := f.tcp.Spec.DeepCopy()
			f.tcp.Spec.ControlPlaneConfig.ControlPlaneConfig.ConfigRevision = "future-intent"
			require.NoError(t, api.Update(t.Context(), f.tcp))
			_, err = f.r.Reconcile(t.Context(), request)
			require.ErrorContains(t, err, "frozen owner intent changed")
			require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.tcp), f.tcp))
			f.tcp.Spec = *frozen
			require.NoError(t, api.Update(t.Context(), f.tcp))
			for _, object := range []client.Object{f.machine, f.config, f.infra} {
				require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(object), object))
				annotations := object.GetAnnotations()
				if annotations == nil {
					annotations = map[string]string{}
				}
				annotations[clusterv1.PausedAnnotation] = ""
				object.SetAnnotations(annotations)
				require.NoError(t, api.Update(t.Context(), object))
				_, err = f.r.Reconcile(t.Context(), request)
				require.NoError(t, err)
				require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.config), f.config))
				require.NotEmpty(t, f.config.Annotations[op.OwnerPlanNameAnnotation], "pause cannot detach a Consumed plan")
				require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(object), object))
				annotations = object.GetAnnotations()
				delete(annotations, clusterv1.PausedAnnotation)
				object.SetAnnotations(annotations)
				require.NoError(t, api.Update(t.Context(), object))
			}
			f.r.Client = &retiringOwnerFaultClient{Client: api, detach: boundary == "detach", remove: boundary == "delete"}
			_, err = f.r.Reconcile(t.Context(), request)
			require.Error(t, err)
			f.r = &TalosControlPlaneReconciler{Client: api, APIReader: api, Scheme: f.r.Scheme, Log: f.r.Log}
			if boundary == "detach" {
				_, err = f.r.Reconcile(t.Context(), request)
				require.NoError(t, err)
			}
			_, remaining, err := f.r.readOwnerPlan(t.Context(), f.tcp)
			require.NoError(t, err)
			require.Nil(t, remaining)
			require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.config), f.config))
			require.Empty(t, f.config.Annotations[op.OwnerPlanNameAnnotation])
			require.Empty(t, f.config.Annotations[op.OwnerPlanUIDAnnotation])
			require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.machine), f.machine))
			require.True(t, f.machine.DeletionTimestamp.IsZero(), "Consumed cleanup itself must not start Machine deletion")
			journal, _, err := op.ReadJournal(t.Context(), api, f.tcp.Namespace, p.Intent.TalosConfig)
			require.NoError(t, err)
			require.NotEmpty(t, journal.Data[op.ConsumedReceiptKey])
		})
	}
}

func exerciseNativeDeletionUIDFence(t *testing.T, api client.Client) {
	f := newOwnerFixture(t, api, "native-delete-uid-fence")
	f.machine.Annotations = map[string]string{PreTerminateHookCleanupAnnotation: ""}
	require.NoError(t, api.Update(t.Context(), f.machine))
	old := f.machine.DeepCopy()
	require.NoError(t, api.Delete(t.Context(), f.machine))
	replacement := old.DeepCopy()
	replacement.UID = ""
	replacement.ResourceVersion = ""
	replacement.ManagedFields = nil
	require.NoError(t, api.Create(t.Context(), replacement))
	require.NotEqual(t, old.UID, replacement.UID)
	require.Error(t, f.r.releasePreTerminateHook(t.Context(), old))
	_, err := f.r.deleteControlPlaneMachine(t.Context(), old)
	require.Error(t, err)
	actual := &clusterv1.Machine{}
	require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(replacement), actual))
	require.Equal(t, replacement.UID, actual.UID)
	require.True(t, actual.DeletionTimestamp.IsZero())
	require.Contains(t, actual.Annotations, PreTerminateHookCleanupAnnotation, "old identity must not release a replacement's hook")
}

func exerciseNativeReleaseIntentFence(t *testing.T, api client.Client) {
	for _, change := range []string{"spec", "node", "pending", "condition", "deletion", "metadata", "lost-response"} {
		t.Run("hook-state-"+change, func(t *testing.T) {
			f := newOwnerFixture(t, api, "hook-state-"+change)
			f.machine.Annotations = map[string]string{PreTerminateHookCleanupAnnotation: ""}
			if change == "deletion" {
				f.machine.Finalizers = []string{"fixture-hold"}
			}
			require.NoError(t, api.Update(t.Context(), f.machine))
			observed := f.machine.DeepCopy()
			live := f.machine.DeepCopy()
			switch change {
			case "spec":
				live.Spec.Version = "v1.35.0"
				require.NoError(t, api.Update(t.Context(), live))
			case "node":
				live.Status.NodeRef = clusterv1.MachineNodeReference{Name: "different-node"}
				require.NoError(t, api.Status().Update(t.Context(), live))
			case "pending":
				live.Annotations[clusterv1.UpdateInProgressAnnotation] = ""
				require.NoError(t, api.Update(t.Context(), live))
			case "condition":
				conditions.Set(live, metav1.Condition{Type: clusterv1.MachineUpdatingCondition, Status: metav1.ConditionTrue, Reason: clusterv1.MachineInPlaceUpdatingReason})
				require.NoError(t, api.Status().Update(t.Context(), live))
			case "deletion":
				require.NoError(t, api.Delete(t.Context(), live))
			case "metadata":
				live.Labels["unrelated"] = "preserved"
				require.NoError(t, api.Update(t.Context(), live))
			}
			if change == "lost-response" {
				f.r.Client = &faultOwnerClient{Client: api, after: 1}
			}
			err := f.r.releasePreTerminateHook(t.Context(), observed)
			if change == "metadata" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			actual := &clusterv1.Machine{}
			require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(live), actual))
			if change == "metadata" {
				require.NotContains(t, actual.Annotations, PreTerminateHookCleanupAnnotation)
				require.Equal(t, "preserved", actual.Labels["unrelated"])
				require.Equal(t, actual.ResourceVersion, observed.ResourceVersion)
			} else if change == "lost-response" {
				require.NotContains(t, actual.Annotations, PreTerminateHookCleanupAnnotation)
				f.r.Client = api
				require.NoError(t, f.r.releasePreTerminateHook(t.Context(), observed))
				require.Equal(t, actual.ResourceVersion, observed.ResourceVersion)
			} else {
				require.Contains(t, actual.Annotations, PreTerminateHookCleanupAnnotation)
			}
		})
	}
}

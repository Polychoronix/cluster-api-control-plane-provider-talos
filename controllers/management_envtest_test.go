// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"crypto"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cosiapi "github.com/cosi-project/runtime/api/v1alpha1"
	cosiserver "github.com/cosi-project/runtime/pkg/state/protobuf/server"
	"github.com/go-logr/logr"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	cp "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const managementPrivateSentinel = "private-generic-management-source-token"

type managementConsumerFixture struct {
	r        *TalosControlPlaneReconciler
	tcp      *cp.TalosControlPlane
	cluster  *clusterv1.Cluster
	machines []*clusterv1.Machine
	configs  []*bootstrapv1.TalosConfig
	objects  []client.Object
	nodes    []corev1.Node
	reads    atomic.Int32
	writes   atomic.Int32
	logs     *ownerRuntimeLogs
	logger   logr.Logger
	recorder *events.FakeRecorder
}

// Every RPC passes strict mTLS and a read-only allowlist. This protocol model
// serves actual Talos/COSI observation responses; it is not a live Talos node.
func managementConsumerNode(t *testing.T, f *managementConsumerFixture, n *ownerRuntimeNode, bundle *secrets.Bundle) bootstrapv1.TalosEndpoint {
	t.Helper()
	ca, err := bundle.Certs.OS.GetCert()
	require.NoError(t, err)
	key, err := bundle.Certs.OS.GetKey()
	require.NoError(t, err)
	cert, _, _ := ownerRuntimeCert(t, ca, key.(crypto.Signer), ownerRuntimeSNI, x509.ExtKeyUsageServerAuth)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	authorize := func(ctx context.Context, method string) error {
		p, ok := peer.FromContext(ctx)
		if !ok {
			return fmt.Errorf("missing TLS peer")
		}
		tlsPeer, ok := p.AuthInfo.(credentials.TLSInfo)
		if !ok || len(tlsPeer.State.VerifiedChains) == 0 || tlsPeer.State.ServerName != ownerRuntimeSNI {
			return fmt.Errorf("missing verified mTLS or expected SNI")
		}
		for _, suffix := range []string{"/Get", "/List", "/ServiceList", "/EtcdMemberList", "/EtcdStatus"} {
			if strings.HasSuffix(method, suffix) {
				f.reads.Add(1)
				return nil
			}
		}
		f.writes.Add(1)
		return fmt.Errorf("unexpected non-observation RPC")
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots})), grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := authorize(ctx, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, request)
	}), grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := authorize(stream.Context(), info.FullMethod); err != nil {
			return err
		}
		return handler(srv, stream)
	}))
	machineapi.RegisterMachineServiceServer(server, n)
	cosiapi.RegisterStateServer(server, cosiserver.NewState(n.resources))
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	return bootstrapv1.TalosEndpoint{Address: listener.Addr().String(), ServerName: ownerRuntimeSNI}
}

func newManagementConsumerFixture(t *testing.T, api client.Client, scheme *runtime.Scheme, count int) *managementConsumerFixture {
	t.Helper()
	namespace := fmt.Sprintf("observe-%d", count)
	require.NoError(t, api.Create(t.Context(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}))
	logs := &ownerRuntimeLogs{}
	logger := zap.New(zap.WriteTo(logs))
	recorder := events.NewFakeRecorder(64)
	f := &managementConsumerFixture{r: &TalosControlPlaneReconciler{Client: api, APIReader: api, Scheme: scheme, Log: logger, Recorder: recorder}, logs: logs, logger: logger, recorder: recorder}
	bundle, err := secrets.NewBundle(secrets.NewFixedClock(time.Now()), config.TalosVersion1_14)
	require.NoError(t, err)
	input, err := generate.NewInput("cluster", "https://api.example.test:6443", "1.35.0", generate.WithVersionContract(config.TalosVersion1_14), generate.WithSecretsBundle(bundle))
	require.NoError(t, err)
	input.PodNet = []string{"10.244.0.0/16"}
	input.ServiceNet = []string{"10.96.0.0/12"}
	identity, err := input.Talosconfig()
	require.NoError(t, err)
	identityBytes, err := identity.Bytes()
	require.NoError(t, err)
	f.cluster = &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: namespace}}
	f.cluster.Spec.ControlPlaneEndpoint = clusterv1.APIEndpoint{Host: "api.example.test", Port: 6443}
	f.cluster.Spec.ClusterNetwork.Pods.CIDRBlocks = input.PodNet
	f.cluster.Spec.ClusterNetwork.Services.CIDRBlocks = input.ServiceNet
	require.NoError(t, api.Create(t.Context(), f.cluster))
	f.tcp = &cp.TalosControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "controlplane", Namespace: namespace, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(f.cluster, clusterv1.GroupVersion.WithKind("Cluster"))}}, Spec: cp.TalosControlPlaneSpec{Replicas: ptr.To(int32(count)), Version: "v1.35.0", Kubeconfig: cp.KubeconfigSpec{Mode: cp.KubeconfigModeExternal}, ControlPlaneConfig: cp.ControlPlaneConfig{ControlPlaneConfig: bootstrapv1.TalosConfigSpec{GenerateType: "controlplane", TalosVersion: "v1.14.0"}}, Management: &bootstrapv1.ManagementSpec{Mode: bootstrapv1.ManagementModeObserve, InventoryRevision: "inventory-1"}}}
	f.tcp.Spec.MachineTemplate.Spec.InfrastructureRef = clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.example.test", Kind: "ObservedMachineTemplate", Name: "never-resolved-in-observe"}
	management := f.tcp.Spec.Management
	f.tcp.Spec.Management = nil
	require.NoError(t, api.Create(t.Context(), f.tcp))
	f.tcp.Spec.Management = management
	memberNames := make([]string, count)
	for i := range count {
		memberNames[i] = fmt.Sprintf("cp-%d", i)
	}
	for i, name := range memberNames {
		seedName := name + "-original-seed"
		m := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: map[string]string{clusterv1.ClusterNameLabel: f.cluster.Name, clusterv1.MachineControlPlaneLabel: ""}, OwnerReferences: []metav1.OwnerReference{{APIVersion: clusterv1.GroupVersion.String(), Kind: "Cluster", Name: f.cluster.Name, UID: f.cluster.UID}}}, Spec: clusterv1.MachineSpec{ClusterName: f.cluster.Name, ProviderID: "metal://" + name, Version: "v1.35.0", InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.example.test", Kind: "ObservedMachine", Name: name}, Bootstrap: clusterv1.Bootstrap{ConfigRef: clusterv1.ContractVersionedObjectReference{APIGroup: bootstrapv1.GroupVersion.Group, Kind: "TalosConfig", Name: name}, DataSecretName: &seedName}}}
		require.NoError(t, api.Create(t.Context(), m))
		m.Status.NodeRef = clusterv1.MachineNodeReference{Name: name}
		require.NoError(t, api.Status().Update(t.Context(), m))
		provider, err := input.Config(machine.TypeControlPlane)
		require.NoError(t, err)
		data, err := provider.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
		require.NoError(t, err)
		data = append(data, []byte("\n# "+managementPrivateSentinel+"\n")...)
		n := ownerRuntimeNewNode()
		n.members = memberNames
		n.memberID = uint64(i + 1)
		require.NoError(t, n.setActive(t.Context(), data))
		require.NoError(t, n.setRuntime(t.Context()))
		endpoint := managementConsumerNode(t, f, n, bundle)
		overlay := &bootstrapv1.MachineConfigOverlay{ConfigSecretRef: &bootstrapv1.SecretKeyReference{Name: name + "-baseline", Key: "config"}}
		f.tcp.Spec.MachineConfigOverrides = append(f.tcp.Spec.MachineConfigOverrides, cp.MachineConfigOverride{Name: name, UID: m.UID, Config: overlay, TalosEndpoint: &endpoint})
		desired, err := f.tcp.Spec.EffectiveTalosConfigSpec(m)
		require.NoError(t, err)
		tc := &bootstrapv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(m, clusterv1.GroupVersion.WithKind("Machine"))}}, Spec: *desired}
		infra := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "infrastructure.example.test/v1beta1", "kind": "ObservedMachine", "metadata": map[string]any{"name": name, "namespace": namespace}, "spec": map[string]any{"providerID": m.Spec.ProviderID, "preserve": "external-inventory"}}}
		infra.SetOwnerReferences(tc.OwnerReferences)
		for _, obj := range []client.Object{tc, infra, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name + "-baseline", Namespace: namespace}, Data: map[string][]byte{"config": data}}, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: seedName, Namespace: namespace}, Data: map[string][]byte{"value": data}}} {
			require.NoError(t, api.Create(t.Context(), obj))
			f.objects = append(f.objects, obj.DeepCopyObject().(client.Object))
		}
		f.machines = append(f.machines, m)
		f.configs = append(f.configs, tc)
		f.objects = append(f.objects, m.DeepCopy(), f.cluster.DeepCopy())
		f.tcp.Spec.Management.Machines = append(f.tcp.Spec.Management.Machines, bootstrapv1.MachineIdentityReference{Name: name, UID: m.UID})
		f.nodes = append(f.nodes, corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("node-" + namespace + "-" + name), Labels: map[string]string{"node-role.kubernetes.io/control-plane": ""}}, Spec: corev1.NodeSpec{ProviderID: m.Spec.ProviderID}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}, NodeInfo: corev1.NodeSystemInfo{KubeletVersion: m.Spec.Version, BootID: "initial-boot"}}})
	}
	require.NoError(t, api.Update(t.Context(), f.tcp))
	workload := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer read-only-workload-token" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api":
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "APIVersions", "apiVersion": "v1", "versions": []string{"v1"}})
		case "/apis":
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "APIGroupList", "apiVersion": "v1", "groups": []any{}})
		case "/api/v1":
			_ = json.NewEncoder(w).Encode(map[string]any{"kind": "APIResourceList", "apiVersion": "v1", "groupVersion": "v1", "resources": []any{map[string]any{"name": "nodes", "kind": "Node", "namespaced": false, "verbs": []string{"get", "list"}}}})
		case "/readyz":
			_, _ = w.Write([]byte("ok"))
		case "/api/v1/nodes":
			_ = json.NewEncoder(w).Encode(corev1.NodeList{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "NodeList"}, Items: f.nodes})
		default:
			for _, node := range f.nodes {
				if r.URL.Path == "/api/v1/nodes/"+node.Name {
					node.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Node"}
					_ = json.NewEncoder(w).Encode(node)
					return
				}
			}
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(workload.Close)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: workload.Certificate().Raw})
	kube := fmt.Sprintf("apiVersion: v1\nkind: Config\nclusters:\n- name: cluster\n  cluster:\n    server: %s\n    certificate-authority-data: %s\ncontexts:\n- name: cluster\n  context:\n    cluster: cluster\n    user: user\ncurrent-context: cluster\nusers:\n- name: user\n  user:\n    token: read-only-workload-token\n", workload.URL, base64.StdEncoding.EncodeToString(ca))
	for _, obj := range []client.Object{&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "cluster-talosconfig", Namespace: namespace}, Data: map[string][]byte{"talosconfig": identityBytes}}, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "cluster-kubeconfig", Namespace: namespace, Labels: map[string]string{clusterv1.ClusterNameLabel: f.cluster.Name}}, Data: map[string][]byte{"value": []byte(kube)}}} {
		require.NoError(t, api.Create(t.Context(), obj))
		f.objects = append(f.objects, obj.DeepCopyObject().(client.Object))
	}
	return f
}

func (f *managementConsumerFixture) reconcile(t *testing.T) {
	t.Helper()
	_, err := f.runReconcile(t)
	require.NoError(t, err)
	require.NoError(t, f.r.APIReader.Get(t.Context(), client.ObjectKeyFromObject(f.tcp), f.tcp))
}

func (f *managementConsumerFixture) runReconcile(t *testing.T) (ctrl.Result, error) {
	t.Helper()
	result, err := f.r.Reconcile(ctrl.LoggerInto(t.Context(), f.logger), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.tcp)})
	if err != nil {
		f.logger.Error(err, "captured returned reconcile error")
		require.NotContains(t, err.Error(), managementPrivateSentinel)
	}
	current := &cp.TalosControlPlane{}
	if readErr := f.r.APIReader.Get(t.Context(), client.ObjectKeyFromObject(f.tcp), current); readErr == nil {
		public, marshalErr := json.Marshal(current.Status)
		require.NoError(t, marshalErr)
		require.NotContains(t, string(public), managementPrivateSentinel)
	} else {
		require.True(t, apierrors.IsNotFound(readErr))
	}
	for {
		select {
		case event := <-f.recorder.Events:
			require.NotContains(t, event, managementPrivateSentinel)
		default:
			require.NotContains(t, f.logs.text(), managementPrivateSentinel)
			return result, err
		}
	}
}
func (f *managementConsumerFixture) preserved(t *testing.T, associated bool) {
	t.Helper()
	for _, before := range f.objects {
		actual := before.DeepCopyObject().(client.Object)
		require.NoError(t, f.r.APIReader.Get(t.Context(), client.ObjectKeyFromObject(before), actual))
		if m, ok := actual.(*clusterv1.Machine); ok && associated {
			m.OwnerReferences = before.GetOwnerReferences()
			m.ResourceVersion = before.GetResourceVersion()
			m.ManagedFields = before.GetManagedFields()
		}
		require.Equal(t, before, actual, "imported object %T %s changed outside approved Machine owners", before, before.GetName())
	}
	require.Empty(t, f.tcp.Finalizers, "Observe/initial association must not add lifecycle finalizers")
	require.Zero(t, f.writes.Load(), "no bootstrap/apply/etcd mutation RPC may run")
}

type associationAckLossClient struct {
	client.Client
	remaining int
}

func (c *associationAckLossClient) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
	err := c.Client.Patch(ctx, obj, p, opts...)
	if _, ok := obj.(*clusterv1.Machine); ok && err == nil {
		c.remaining--
		if c.remaining == 0 {
			return fmt.Errorf("injected lost owner association acknowledgment")
		}
	}
	return err
}

func TestControlPlaneManagementObserveAssociationEnvtest(t *testing.T) {
	if testing.Short() {
		t.Skip("isolated actual API/TLS management smoke")
	}
	env := &envtest.Environment{UseExistingCluster: ptr.To(false), CRDDirectoryPaths: []string{filepath.Join("..", "config", "crd", "bases"), filepath.Join(moduleDirectory(t, "sigs.k8s.io/cluster-api"), "core", "config", "crd", "bases"), filepath.Join(moduleDirectory(t, "github.com/siderolabs/cluster-api-bootstrap-provider-talos"), "config", "crd", "bases")}, ErrorIfCRDPathMissing: true, CRDs: []*apiextensionsv1.CustomResourceDefinition{{ObjectMeta: metav1.ObjectMeta{Name: "observedmachines.infrastructure.example.test", Labels: map[string]string{"cluster.x-k8s.io/v1beta2": "v1beta1"}}, Spec: apiextensionsv1.CustomResourceDefinitionSpec{Group: "infrastructure.example.test", Scope: apiextensionsv1.NamespaceScoped, Names: apiextensionsv1.CustomResourceDefinitionNames{Plural: "observedmachines", Singular: "observedmachine", Kind: "ObservedMachine"}, Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{Name: "v1beta1", Served: true, Storage: true, Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{Type: "object", XPreserveUnknownFields: ptr.To(true)}}}}}}}}
	cfg, err := env.Start()
	if err != nil && os.Getenv("KUBEBUILDER_ASSETS") == "" && strings.Contains(err.Error(), "fork/exec") {
		t.Skipf("envtest assets not installed: %v", err)
	}
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, bootstrapv1.AddToScheme(scheme))
	require.NoError(t, cp.AddToScheme(scheme))
	api, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	for _, count := range []int{2, 3} {
		t.Run(fmt.Sprintf("members-%d", count), func(t *testing.T) {
			f := newManagementConsumerFixture(t, api, scheme, count)
			f.reconcile(t)
			require.True(t, conditions.IsTrue(f.tcp, bootstrapv1.InventoryVerifiedCondition))
			require.False(t, conditions.IsTrue(f.tcp, bootstrapv1.ManagementActivatedCondition))
			require.False(t, conditions.IsTrue(f.tcp, clusterv1.ReadyCondition))
			require.False(t, conditions.IsTrue(f.tcp, cp.AvailableCondition))
			require.Empty(t, f.tcp.Status.Management.ActivatedRevision)
			id := f.tcp.Status.Management.Observation.ID
			require.NotEmpty(t, id)
			require.Positive(t, f.reads.Load())
			f.preserved(t, false)
			f.reconcile(t)
			require.Equal(t, id, f.tcp.Status.Management.Observation.ID, "healthy resync preserves observation consent identity")
			f.preserved(t, false)
			for _, nodePatch := range []bool{false, true} {
				if nodePatch {
					f.tcp.Spec.MachineConfigOverrides[0].Config.StrategicPatches = []string{"machine:\n  env:\n    NOT_APPLIED: desired-node-policy\n"}
				} else {
					f.tcp.Spec.ControlPlaneConfig.ControlPlaneConfig.StrategicPatches = []string{"machine:\n  env:\n    NOT_APPLIED: desired-shared-policy\n"}
				}
				require.NoError(t, api.Update(t.Context(), f.tcp))
				f.reconcile(t)
				require.False(t, conditions.IsTrue(f.tcp, bootstrapv1.InventoryVerifiedCondition), "unapplied effective shared/node policy must not verify")
				require.False(t, conditions.IsTrue(f.tcp, bootstrapv1.ManagementActivatedCondition))
				for _, m := range f.machines {
					require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(m), m))
					require.Nil(t, metav1.GetControllerOf(m))
				}
				f.preserved(t, false)
				f.tcp.Spec.MachineConfigOverrides[0].Config.StrategicPatches = nil
				f.tcp.Spec.ControlPlaneConfig.ControlPlaneConfig.StrategicPatches = nil
				require.NoError(t, api.Update(t.Context(), f.tcp))
				f.reconcile(t)
				require.Equal(t, id, f.tcp.Status.Management.Observation.ID)
			}
			// Effective policy need not have the live TC's raw representation.
			// An equivalent desired-only Secret patch must be included in safety pins.
			equivalent := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "effective-equivalent", Namespace: f.tcp.Namespace}, Data: map[string][]byte{"patch": []byte("machine:\n  type: controlplane\n# " + managementPrivateSentinel + "\n")}}
			require.NoError(t, api.Create(t.Context(), equivalent))
			f.tcp.Spec.ControlPlaneConfig.ControlPlaneConfig.StrategicPatchSecretRefs = []bootstrapv1.SecretKeyReference{{Name: equivalent.Name, Key: "patch"}}
			require.NoError(t, api.Update(t.Context(), f.tcp))
			f.reconcile(t)
			safePatch := append([]byte(nil), equivalent.Data["patch"]...)
			equivalent.Data["patch"] = []byte("machine: [\n# " + managementPrivateSentinel + "\n")
			require.NoError(t, api.Update(t.Context(), equivalent))
			f.reconcile(t)
			require.False(t, conditions.IsTrue(f.tcp, bootstrapv1.InventoryVerifiedCondition))
			equivalent.Data["patch"] = safePatch
			require.NoError(t, api.Update(t.Context(), equivalent))
			f.reconcile(t)
			require.True(t, conditions.IsTrue(f.tcp, bootstrapv1.InventoryVerifiedCondition))
			require.NotEqual(t, id, f.tcp.Status.Management.Observation.ID)
			id = f.tcp.Status.Management.Observation.ID
			equivalent.Labels = map[string]string{"rotated": "true"}
			require.NoError(t, api.Update(t.Context(), equivalent))
			f.reconcile(t)
			require.NotEqual(t, id, f.tcp.Status.Management.Observation.ID, "effective-only source RV changes must invalidate consent")
			id = f.tcp.Status.Management.Observation.ID
			f.objects = append(f.objects, equivalent.DeepCopy())
			foreign := f.machines[0].DeepCopy()
			require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(foreign), foreign))
			originalOwners := append([]metav1.OwnerReference(nil), foreign.OwnerReferences...)
			foreign.OwnerReferences = append(foreign.OwnerReferences, metav1.OwnerReference{APIVersion: cp.GroupVersion.String(), Kind: "TalosControlPlane", Name: "foreign", UID: "foreign-owner", Controller: ptr.To(true)})
			require.NoError(t, api.Update(t.Context(), foreign))
			beforeReads := f.reads.Load()
			f.reconcile(t)
			require.Equal(t, beforeReads, f.reads.Load(), "foreign controller must stop before Talos observation")
			require.False(t, conditions.IsTrue(f.tcp, bootstrapv1.InventoryVerifiedCondition))
			require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(foreign), foreign))
			require.Equal(t, "foreign-owner", string(metav1.GetControllerOf(foreign).UID), "observation never steals ownership")
			foreign.OwnerReferences = originalOwners
			require.NoError(t, api.Update(t.Context(), foreign))
			f.reconcile(t)
			require.Equal(t, id, f.tcp.Status.Management.Observation.ID)
			var pausedInfra client.Object
			for _, object := range f.objects {
				if _, ok := object.(*unstructured.Unstructured); ok {
					pausedInfra = object
				}
			}
			require.NotNil(t, pausedInfra)
			for _, pause := range []client.Object{f.cluster, f.tcp, f.machines[len(f.machines)-1], f.configs[len(f.configs)-1], pausedInfra} {
				live := pause.DeepCopyObject().(client.Object)
				require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(live), live))
				live.SetAnnotations(map[string]string{clusterv1.PausedAnnotation: ""})
				require.NoError(t, api.Update(t.Context(), live))
				beforeReads := f.reads.Load()
				f.reconcile(t)
				require.Equal(t, beforeReads, f.reads.Load(), "pause must stop before remote observation")
				require.Equal(t, id, f.tcp.Status.Management.Observation.ID)
				require.True(t, conditions.IsTrue(f.tcp, clusterv1.PausedCondition))
				require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(live), live))
				live.SetAnnotations(nil)
				require.NoError(t, api.Update(t.Context(), live))
				f.reconcile(t)
				require.Equal(t, id, f.tcp.Status.Management.Observation.ID)
			}
			clusterPaused := f.cluster.DeepCopy()
			require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(clusterPaused), clusterPaused))
			clusterPaused.Spec.Paused = ptr.To(true)
			require.NoError(t, api.Update(t.Context(), clusterPaused))
			pauseReads := f.reads.Load()
			f.reconcile(t)
			require.Equal(t, pauseReads, f.reads.Load())
			require.Equal(t, id, f.tcp.Status.Management.Observation.ID)
			clusterPaused.Spec.Paused = nil
			require.NoError(t, api.Update(t.Context(), clusterPaused))
			f.reconcile(t)
			for _, object := range f.objects {
				if _, ok := object.(*clusterv1.Cluster); ok {
					object.SetGeneration(clusterPaused.Generation)
				}
			}
			for _, before := range f.objects {
				actual := before.DeepCopyObject().(client.Object)
				require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(before), actual))
				rv := actual.GetResourceVersion()
				managedFields := actual.GetManagedFields()
				actual.SetManagedFields(before.GetManagedFields())
				actual.SetResourceVersion(before.GetResourceVersion())
				require.Equal(t, before, actual, "intentional hard-pause metadata updates must be the only object changes")
				before.SetResourceVersion(rv)
				before.SetManagedFields(managedFields)
			}
			// Same-name/same-byte source rotation changes freshness and invalidates old consent.
			baseline := &corev1.Secret{}
			require.NoError(t, api.Get(t.Context(), client.ObjectKey{Namespace: f.tcp.Namespace, Name: "cp-0-baseline"}, baseline))
			baseline.Labels = map[string]string{"rotated": "true"}
			require.NoError(t, api.Update(t.Context(), baseline))
			f.reconcile(t)
			newID := f.tcp.Status.Management.Observation.ID
			require.NotEqual(t, id, newID)
			f.tcp.Spec.Management.Mode = bootstrapv1.ManagementModeManaged
			f.tcp.Spec.Management.Approval = &bootstrapv1.ObservationApproval{Revision: "inventory-1", ObservationID: id}
			require.NoError(t, api.Update(t.Context(), f.tcp))
			f.reconcile(t)
			for _, m := range f.machines {
				require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(m), m))
				require.Nil(t, metav1.GetControllerOf(m), "stale approval cannot claim a Machine")
			}
			f.tcp.Spec.Management.Approval.ObservationID = newID
			require.NoError(t, api.Update(t.Context(), f.tcp))
			f.r.Client = &associationAckLossClient{Client: api, remaining: 1}
			_, err := f.runReconcile(t)
			require.ErrorContains(t, err, "compare-and-swap")
			status, err := op.ReadManagementStatus(t.Context(), api, tcpManagementAuthority(f.tcp))
			require.NoError(t, err)
			require.Empty(t, status.ActivatedRevision, "partial owner transfer is never active")
			require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.machines[0]), f.machines[0]))
			require.True(t, machineIsControlledByTCP(f.machines[0], f.tcp))
			require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.machines[1]), f.machines[1]))
			require.Nil(t, metav1.GetControllerOf(f.machines[1]))
			// An unhealthy remaining member stops crash recovery before further ownership writes.
			f.nodes[len(f.nodes)-1].Status.Conditions[0].Status = corev1.ConditionFalse
			f.r = &TalosControlPlaneReconciler{Client: api, APIReader: api, Scheme: scheme, Log: f.logger, Recorder: f.recorder}
			f.reconcile(t)
			status, err = op.ReadManagementStatus(t.Context(), api, tcpManagementAuthority(f.tcp))
			require.NoError(t, err)
			require.Empty(t, status.ActivatedRevision)
			require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.machines[1]), f.machines[1]))
			require.Nil(t, metav1.GetControllerOf(f.machines[1]))
			f.nodes[len(f.nodes)-1].Status.Conditions[0].Status = corev1.ConditionTrue
			f.reconcile(t)
			require.Equal(t, "inventory-1", f.tcp.Status.Management.ActivatedRevision)
			require.Equal(t, newID, f.tcp.Status.Management.Observation.ID, "ownership rebind preserves original approved observation")
			require.True(t, conditions.IsTrue(f.tcp, bootstrapv1.ManagementActivatedCondition))
			// Refresh only the intentionally rotated source fixture before preservation checks.
			for i, obj := range f.objects {
				if s, ok := obj.(*corev1.Secret); ok && s.Name == baseline.Name {
					f.objects[i] = baseline.DeepCopy()
				}
			}
			f.preserved(t, true)
			approved := f.tcp.Spec.Management.DeepCopy()
			f.tcp.Spec.Management = nil
			f.tcp.Status.Management = nil
			require.NoError(t, api.Update(t.Context(), f.tcp))
			require.NoError(t, api.Status().Update(t.Context(), f.tcp))
			_, err = f.runReconcile(t)
			require.ErrorContains(t, err, "retained record")
			require.Zero(t, f.writes.Load())
			require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.tcp), f.tcp))
			f.tcp.Spec.Management = approved
			require.NoError(t, api.Update(t.Context(), f.tcp))
			exerciseManagementRetirement(t, api, f)
			require.NotEmpty(t, f.logs.text(), "actual scoped Reconcile logs must be captured")
			require.NotContains(t, f.logs.text(), managementPrivateSentinel)
			t.Log("actual provider Reconcile: full generic API inventory + workload TLS Node reads + strict Talos mTLS/COSI/etcd observation; hard pauses, source freshness/stale approval, owner CAS response loss/restart/health hold and aggregate activation; no live node/core lifecycle provisioning claim")
		})
	}
}

// Actual deleting-TCP/Machine API requests exercise native retirement without
// fabricated infrastructure readiness, core deletion finalizers or physical reset.
func exerciseManagementRetirement(t *testing.T, api client.Client, f *managementConsumerFixture) {
	f.reconcile(t)
	require.Contains(t, f.tcp.Finalizers, cp.TalosControlPlaneFinalizer, "activated inventory must acquire native lifecycle protection")
	beforeReads := f.reads.Load()
	require.NoError(t, api.Delete(t.Context(), f.tcp))
	require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(f.tcp), f.tcp))
	m := f.machines[0]
	require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(m), m))
	m.Annotations = map[string]string{clusterv1.UpdateInProgressAnnotation: ""}
	require.NoError(t, api.Update(t.Context(), m))
	_, err := f.runReconcile(t)
	require.ErrorContains(t, err, "active Machine update")
	require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(m), m))
	require.True(t, m.DeletionTimestamp.IsZero())
	m.Annotations = nil
	require.NoError(t, api.Update(t.Context(), m))
	last := f.configs[len(f.configs)-1]
	require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(last), last))
	last.Annotations = map[string]string{clusterv1.PausedAnnotation: ""}
	require.NoError(t, api.Update(t.Context(), last))
	_, err = f.runReconcile(t)
	require.NoError(t, err)
	require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(m), m))
	require.True(t, m.DeletionTimestamp.IsZero(), "paused retirement may not delete any member")
	last.Annotations = nil
	require.NoError(t, api.Update(t.Context(), last))
	// Native core owns drain/Node disappearance; retirement must not demand a
	// new healthy-running snapshot or perform a Talos membership/reset operation.
	f.nodes[len(f.nodes)-1].Status.Conditions[0].Status = corev1.ConditionFalse
	for range 3 {
		_, err = f.runReconcile(t)
		require.NoError(t, err)
	}
	for _, machine := range f.machines {
		require.True(t, apierrors.IsNotFound(api.Get(t.Context(), client.ObjectKeyFromObject(machine), &clusterv1.Machine{})), "native retirement should request actual API deletion")
	}
	require.True(t, apierrors.IsNotFound(api.Get(t.Context(), client.ObjectKeyFromObject(f.tcp), &cp.TalosControlPlane{})), "native finalizer removal should complete TCP API deletion")
	require.Equal(t, beforeReads, f.reads.Load(), "retirement is not permission for renewed Talos observation or mutation")
	require.Zero(t, f.writes.Load())
	for _, object := range f.objects {
		if _, ok := object.(*clusterv1.Machine); ok {
			continue
		}
		if _, ok := object.(*bootstrapv1.TalosConfig); ok {
			continue
		} // Explicit pause metadata was test-written.
		actual := object.DeepCopyObject().(client.Object)
		require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(object), actual))
		require.Equal(t, object, actual, "retirement must not alter infrastructure/seeds/credentials")
	}
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	bootstrapalpha "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1alpha3"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	cpalpha "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1alpha3"
	cp "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-control-plane-provider-talos/internal/runtimeclient"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	featuretesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/utils/ptr"
	clusterv1alpha "sigs.k8s.io/cluster-api/api/core/v1beta1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimealpha "sigs.k8s.io/cluster-api/api/runtime/v1alpha1"
	runtimev1 "sigs.k8s.io/cluster-api/api/runtime/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/feature"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

// Explicit opt-in: actual managers use only generated local credentials. Node
// runtime/provisioning is simulated; no physical or three-node rollout proof.
func TestOwnerActualCABPTCoreReceiptCoupling(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	coreBinary := os.Getenv("CAPI_RUNTIME_CORE_BINARY")
	source := os.Getenv("CAPI_RUNTIME_CORE_SOURCE")
	caph := os.Getenv("CAPI_RUNTIME_CAPH_CRDS")
	bootstrapBinary := os.Getenv("CABPT_RUNTIME_BINARY")
	if coreBinary == "" && source == "" && caph == "" && bootstrapBinary == "" {
		t.Skip("set explicit core/source/CAPH/CABPT runtime paths for isolated owner coupling")
	}
	for _, p := range []string{assets, coreBinary, source, caph, bootstrapBinary} {
		require.NotEmpty(t, p)
		require.True(t, filepath.IsAbs(p))
		_, e := os.Stat(p)
		require.NoError(t, e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	ownerLogs := &ownerRuntimeLogs{}
	ownerLogger := zap.New(zap.WriteTo(ownerLogs))
	ctx = ctrl.LoggerInto(ctx, ownerLogger)
	t.Cleanup(func() {
		require.NotEmpty(t, ownerLogs.text())
		require.NotContains(t, ownerLogs.text(), ownerRuntimePrivate)
	})
	featuretesting.SetFeatureGateDuringTest(t, feature.Gates, feature.InPlaceUpdates, true)
	scheme := kruntime.NewScheme()
	for _, add := range []func(*kruntime.Scheme) error{corev1.AddToScheme, apiextv1.AddToScheme, clusterv1.AddToScheme, clusterv1alpha.AddToScheme, runtimev1.AddToScheme, runtimealpha.AddToScheme, bootstrapv1.AddToScheme, bootstrapalpha.AddToScheme, cp.AddToScheme, cpalpha.AddToScheme} {
		require.NoError(t, add(scheme))
	}
	env := &envtest.Environment{UseExistingCluster: ptr.To(false), BinaryAssetsDirectory: assets, Scheme: scheme, ErrorIfCRDPathMissing: true, CRDDirectoryPaths: []string{filepath.Join(source, "core/config/crd/bases")}, WebhookInstallOptions: envtest.WebhookInstallOptions{Paths: []string{filepath.Join(source, "core/config/webhook/manifests.yaml")}, LocalServingHost: "127.0.0.1", IgnoreSchemeConvertible: true}}
	cfg, e := env.Start()
	require.NoError(t, e)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	bootstrapRepo := moduleDirectory(t, "github.com/siderolabs/cluster-api-bootstrap-provider-talos")
	install := func(repo string) envtest.WebhookInstallOptions {
		opts := envtest.WebhookInstallOptions{Paths: []string{filepath.Join(repo, "config/webhook/manifests.yaml")}, LocalServingHost: "127.0.0.1"}
		require.NoError(t, opts.PrepWithoutInstalling())
		require.NoError(t, opts.Install(cfg))
		crds := envtest.CRDInstallOptions{Scheme: scheme, Paths: []string{filepath.Join(repo, "config/crd/bases")}, ErrorIfPathMissing: true, WebhookOptions: opts}
		require.NoError(t, envtest.ReadCRDFiles(&crds))
		for _, crd := range crds.CRDs {
			if crd.Labels == nil {
				crd.Labels = map[string]string{}
			}
			crd.Labels["cluster.x-k8s.io/v1beta2"] = "v1beta1"
			crd.Labels["cluster.x-k8s.io/v1beta1"] = "v1beta1"
		}
		crds.Paths = nil
		_, e := envtest.InstallCRDs(cfg, crds)
		require.NoError(t, e)
		return opts
	}
	bootstrapWebhook := install(bootstrapRepo)
	// CACPPT manifests are already rooted at this package's repository.
	cwd, e := os.Getwd()
	require.NoError(t, e)
	cpWebhook := install(filepath.Dir(cwd))
	caphOpts := envtest.CRDInstallOptions{Paths: []string{caph}, Scheme: scheme, ErrorIfPathMissing: true}
	require.NoError(t, envtest.ReadCRDFiles(&caphOpts))
	for _, crd := range caphOpts.CRDs {
		if crd.Labels == nil {
			crd.Labels = map[string]string{}
		}
		crd.Labels["cluster.x-k8s.io/v1beta1"] = "v1beta1"
	}
	caphOpts.Paths = nil
	_, e = envtest.InstallCRDs(cfg, caphOpts)
	require.NoError(t, e)
	api, e := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, e)
	mgr, e := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Logger: ownerLogger, Metrics: metricsserver.Options{BindAddress: "0"}, HealthProbeBindAddress: "0", WebhookServer: webhook.NewServer(webhook.Options{Host: "127.0.0.1", Port: cpWebhook.LocalServingPort, CertDir: cpWebhook.LocalServingCertDir})})
	require.NoError(t, e)
	require.NoError(t, (&cp.TalosControlPlane{}).SetupWebhookWithManager(mgr))
	require.NoError(t, (&cp.TalosControlPlaneTemplate{}).SetupWebhookWithManager(mgr))
	cpalpha.SetAPIVersionGetter(func(gk schema.GroupKind) (string, error) {
		mapping, e := mgr.GetRESTMapper().RESTMapping(gk)
		if e != nil {
			return "", e
		}
		return mapping.GroupVersionKind.GroupVersion().String(), nil
	})
	cache, e := clustercache.SetupWithManager(ctx, mgr, clustercache.Options{SecretClient: api, Client: clustercache.ClientOptions{UserAgent: "owner-real-core", Timeout: time.Second, Cache: clustercache.ClientCacheOptions{DisableFor: []client.Object{&corev1.Node{}, &corev1.Pod{}}}}}, controller.Options{MaxConcurrentReconciles: 1})
	require.NoError(t, e)
	managerCtx, stopManager := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- mgr.Start(managerCtx) }()
	t.Cleanup(func() { stopManager(); require.NoError(t, <-done) })
	configPath := filepath.Join(t.TempDir(), "management.kubeconfig")
	require.NoError(t, os.WriteFile(configPath, env.KubeConfig, 0600))
	runtimeDir, runtimeCA := ownerRuntimeCertDir(t)
	runtimePort := ownerRuntimePort(t)
	bootstrapLogs := ownerRuntimeProcess(t, ctx, bootstrapBinary, []string{"--kubeconfig=" + configPath, "--webhook-port=" + strconv.Itoa(bootstrapWebhook.LocalServingPort), "--webhook-cert-dir=" + bootstrapWebhook.LocalServingCertDir, "--runtime-extension-port=" + strconv.Itoa(runtimePort), "--runtime-extension-cert-dir=" + runtimeDir, "--health-addr=127.0.0.1:" + strconv.Itoa(ownerRuntimePort(t)), "--enable-machine-pool-in-place-updates=false"})
	health := ownerRuntimePort(t)
	coreLogs := ownerRuntimeProcess(t, ctx, coreBinary, []string{"--kubeconfig=" + configPath, "--feature-gates=RuntimeSDK=true,InPlaceUpdates=true,ClusterTopology=false,MachinePool=false", "--leader-elect=false", "--webhook-port=" + strconv.Itoa(env.WebhookInstallOptions.LocalServingPort), "--webhook-cert-dir=" + env.WebhookInstallOptions.LocalServingCertDir, "--health-addr=127.0.0.1:" + strconv.Itoa(health), "--diagnostics-address=0", "--cluster-concurrency=1", "--clustercache-concurrency=1", "--machine-concurrency=1", "--machineset-concurrency=1", "--machinedeployment-concurrency=1", "--extensionconfig-concurrency=1", "--clusterresourceset-concurrency=1", "--machinehealthcheck-concurrency=1", "--v=2"})
	ns := "owner-coupling"
	ownerRuntimeFailureState = func() string { return ownerRuntimeSnapshot(ctx, api, ns, coreLogs, bootstrapLogs) }
	t.Cleanup(func() { ownerRuntimeFailureState = nil })
	ownerRuntimeEventually(t, "core admission readiness", 20*time.Second, func() error {
		res, e := (&http.Client{Timeout: time.Second}).Get("http://127.0.0.1:" + strconv.Itoa(health) + "/readyz")
		if e != nil {
			return e
		}
		res.Body.Close()
		if res.StatusCode != 200 {
			return fmt.Errorf("not ready")
		}
		return nil
	})
	require.NoError(t, api.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, Labels: map[string]string{"owner-coupling": "yes"}}}))
	ec := &runtimev1.ExtensionConfig{ObjectMeta: metav1.ObjectMeta{Name: "owner-coupling"}, Spec: runtimev1.ExtensionConfigSpec{ClientConfig: runtimev1.ClientConfig{URL: "https://127.0.0.1:" + strconv.Itoa(runtimePort), CABundle: runtimeCA}, NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"owner-coupling": "yes"}}}}
	ownerRuntimeEventually(t, "create runtime registration", 20*time.Second, func() error { return api.Create(ctx, ec) })
	ownerRuntimeEventually(t, "actual CABPT Discovery", 30*time.Second, func() error {
		if e := api.Get(ctx, client.ObjectKeyFromObject(ec), ec); e != nil {
			return e
		}
		if len(ec.Status.Handlers) != 3 {
			return fmt.Errorf("handlers %d", len(ec.Status.Handlers))
		}
		return nil
	})
	clusterName := "coupling"
	infraCluster := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "infrastructure.cluster.x-k8s.io/v1beta1", "kind": "HetznerCluster", "metadata": map[string]any{"name": clusterName, "namespace": ns}, "spec": map[string]any{"controlPlaneRegions": []any{"fsn1"}, "hetznerSecretRef": map[string]any{"name": "unused", "key": map[string]any{"hcloudToken": "token"}}, "sshKeys": map[string]any{}, "hcloudNetwork": map[string]any{"enabled": false}, "controlPlaneLoadBalancer": map[string]any{"enabled": false}}}}
	require.NoError(t, api.Create(ctx, infraCluster))
	beforeInfra := infraCluster.DeepCopy()
	infraCluster.Object["status"] = map[string]any{"ready": true}
	require.NoError(t, api.Status().Patch(ctx, infraCluster, client.MergeFrom(beforeInfra)))
	url, e := url.Parse(cfg.Host)
	require.NoError(t, e)
	host, port, e := net.SplitHostPort(url.Host)
	require.NoError(t, e)
	p, e := strconv.Atoi(port)
	require.NoError(t, e)
	tcp := &cp.TalosControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "coupling", Namespace: ns}, Spec: cp.TalosControlPlaneSpec{Version: "v1.35.0", Replicas: ptr.To(int32(2)), Kubeconfig: cp.KubeconfigSpec{Mode: cp.KubeconfigModeExternal}}}
	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: ns}, Spec: clusterv1.ClusterSpec{ControlPlaneEndpoint: clusterv1.APIEndpoint{Host: host, Port: int32(p)}, ControlPlaneRef: clusterv1.ContractVersionedObjectReference{APIGroup: cp.GroupVersion.Group, Kind: "TalosControlPlane", Name: tcp.Name}, InfrastructureRef: clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.cluster.x-k8s.io", Kind: "HetznerCluster", Name: infraCluster.GetName()}}}
	localKube := clientcmdapi.NewConfig()
	localKube.Clusters[clusterName] = &clientcmdapi.Cluster{Server: cfg.Host, CertificateAuthorityData: cfg.CAData}
	localKube.AuthInfos["envtest"] = &clientcmdapi.AuthInfo{ClientCertificateData: cfg.CertData, ClientKeyData: cfg.KeyData}
	localKube.Contexts["local"] = &clientcmdapi.Context{Cluster: clusterName, AuthInfo: "envtest"}
	localKube.CurrentContext = "local"
	kube, e := clientcmd.Write(*localKube)
	require.NoError(t, e)
	require.NoError(t, clientcmd.Validate(*localKube))
	require.NoError(t, api.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: clusterName + "-kubeconfig", Namespace: ns, Labels: map[string]string{clusterv1.ClusterNameLabel: clusterName}}, Data: map[string][]byte{"value": kube}}))
	require.NoError(t, api.Create(ctx, cluster))
	bundle, e := secrets.NewBundle(secrets.NewClock(), config.TalosVersion1_14)
	require.NoError(t, e)
	nodes := []*ownerRuntimeNode{ownerRuntimeNewNode(), ownerRuntimeNewNode()}
	endpoints := make([]bootstrapv1.TalosEndpoint, 2)
	var identity []byte
	for i := range nodes {
		var data []byte
		endpoints[i], data = ownerRuntimeStartNode(t, nodes[i], bundle, clusterName)
		if i == 0 {
			identity = data
		}
		require.NoError(t, nodes[i].setRuntime(ctx))
	}
	require.NoError(t, api.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: clusterName + "-talosconfig", Namespace: ns}, Data: map[string][]byte{"talosconfig": identity}}))
	input, e := generate.NewInput(clusterName, cfg.Host, "1.35.0", generate.WithVersionContract(config.TalosVersion1_14), generate.WithSecretsBundle(bundle))
	require.NoError(t, e)
	baseline, e := input.Config(machine.TypeControlPlane)
	require.NoError(t, e)
	baselineData, e := baseline.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
	require.NoError(t, e)
	require.NoError(t, api.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "baseline", Namespace: ns}, Data: map[string][]byte{"value": baselineData}}))
	template := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "infrastructure.cluster.x-k8s.io/v1beta1", "kind": "HetznerBareMetalMachineTemplate", "metadata": map[string]any{"name": "installed", "namespace": ns}, "spec": map[string]any{"template": map[string]any{"spec": map[string]any{"installImage": map[string]any{"image": map[string]any{"url": "https://example.invalid/not-installed.raw"}, "partitions": []any{}}}}}}}
	require.NoError(t, api.Create(ctx, template))
	tcp.Spec.MachineTemplate.Spec.InfrastructureRef = clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.cluster.x-k8s.io", Kind: template.GetKind(), Name: template.GetName()}
	tcp.Spec.ControlPlaneConfig.ControlPlaneConfig = bootstrapv1.TalosConfigSpec{GenerateType: "controlplane", TalosVersion: "v1.14.0", ConfigSecretRef: &bootstrapv1.SecretKeyReference{Name: "baseline", Key: "value"}, ConfigRevision: "initial", TalosEndpoint: &endpoints[0], Hostname: bootstrapv1.HostnameSpec{Source: bootstrapv1.HostnameSourceMachineName}}
	tcp.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(cluster, clusterv1.GroupVersion.WithKind("Cluster"))}
	tcp.Labels = map[string]string{clusterv1.ClusterNameLabel: clusterName}
	tcp.Finalizers = []string{cp.TalosControlPlaneFinalizer}
	require.NoError(t, api.Create(ctx, tcp))
	r := &TalosControlPlaneReconciler{Client: api, APIReader: api, Scheme: scheme, Log: ownerLogger, ClusterCache: cache, RuntimeClient: runtimeclient.New(api)}
	var machines []*clusterv1.Machine
	var configs []*bootstrapv1.TalosConfig
	for i := range nodes {
		if i == 1 {
			require.NoError(t, api.Get(ctx, client.ObjectKeyFromObject(tcp), tcp))
			before := tcp.DeepCopy()
			tcp.Spec.MachineConfigOverrides = []cp.MachineConfigOverride{{Name: machines[0].Name, UID: machines[0].UID, TalosEndpoint: &endpoints[0]}}
			tcp.Spec.ControlPlaneConfig.ControlPlaneConfig.TalosEndpoint = &endpoints[1]
			require.NoError(t, api.Patch(ctx, tcp, client.MergeFrom(before)))
		}
		// Actual fresh creator, not retrospective config/pivot marking. Infrastructure
		// and Node readiness are not supplied until the real renderer emits lineage.
		_, e := r.bootControlPlane(ctx, cluster, tcp)
		require.NoError(t, e)
		var created *clusterv1.Machine
		ownerRuntimeEventually(t, "fresh Machine", 20*time.Second, func() error {
			list := &clusterv1.MachineList{}
			if e := api.List(ctx, list, client.InNamespace(ns)); e != nil {
				return e
			}
			for j := range list.Items {
				candidate := &list.Items[j]
				if len(machines) == 0 || candidate.UID != machines[0].UID {
					created = candidate.DeepCopy()
				}
			}
			if created == nil {
				return fmt.Errorf("Machine not created")
			}
			return nil
		})
		tc := &bootstrapv1.TalosConfig{}
		seed := &corev1.Secret{}
		ownerRuntimeEventually(t, "real initial renderer accepted lineage", 30*time.Second, func() error {
			if e := api.Get(ctx, client.ObjectKey{Namespace: ns, Name: created.Spec.Bootstrap.ConfigRef.Name}, tc); e != nil {
				return e
			}
			if tc.Status.DataSecretName == "" {
				return fmt.Errorf("no initial seed")
			}
			if e := api.Get(ctx, client.ObjectKey{Namespace: ns, Name: tc.Status.DataSecretName}, seed); e != nil {
				return e
			}
			accepted, e := op.DecodeAcceptedIntent(seed.Data[op.AcceptedIntentKey])
			if e != nil {
				return fmt.Errorf("accepted intent absent")
			}
			if accepted.MachineUID != created.UID || accepted.TalosConfigUID != tc.UID || accepted.TransactionID != "" {
				return fmt.Errorf("wrong initial lineage")
			}
			return nil
		})
		require.NoError(t, nodes[i].setActive(ctx, seed.Data["value"]))
		machines = append(machines, created)
		configs = append(configs, tc)
	}
	names := []string{machines[0].Name, machines[1].Name}
	for index, node := range nodes {
		node.mu.Lock()
		node.members = names
		node.memberID = uint64(index + 1)
		node.mu.Unlock()
	}
	for i, m := range machines {
		infra := &unstructured.Unstructured{}
		infra.SetAPIVersion("infrastructure.cluster.x-k8s.io/v1beta1")
		infra.SetKind("HetznerBareMetalMachine")
		require.NoError(t, api.Get(ctx, client.ObjectKey{Namespace: ns, Name: m.Spec.InfrastructureRef.Name}, infra))
		before := infra.DeepCopy()
		providerID := fmt.Sprintf("hcloud://bm-%d", i+1)
		require.NoError(t, unstructured.SetNestedField(infra.Object, providerID, "spec", "providerID"))
		require.NoError(t, api.Patch(ctx, infra, client.MergeFrom(before)))
		before = infra.DeepCopy()
		infra.Object["status"] = map[string]any{"ready": true}
		require.NoError(t, api.Status().Patch(ctx, infra, client.MergeFrom(before)))
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: m.Name, Labels: map[string]string{"node-role.kubernetes.io/control-plane": ""}}, Spec: corev1.NodeSpec{ProviderID: providerID}}
		require.NoError(t, api.Create(ctx, node))
		node.Status = corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{KubeletVersion: "v1.35.0"}, Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}, {Type: corev1.NodeMemoryPressure, Status: corev1.ConditionFalse}, {Type: corev1.NodeDiskPressure, Status: corev1.ConditionFalse}, {Type: corev1.NodePIDPressure, Status: corev1.ConditionFalse}}}
		require.NoError(t, api.Status().Update(ctx, node))
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "kube-apiserver-" + m.Name, Namespace: "kube-system", Labels: map[string]string{"component": "kube-apiserver"}}, Spec: corev1.PodSpec{NodeName: m.Name, Containers: []corev1.Container{{Name: "kube-apiserver", Image: "registry/kube-apiserver:v1.35.0"}}}}
		require.NoError(t, api.Create(ctx, pod))
		pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "kube-apiserver", Image: "registry/kube-apiserver:v1.35.0", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}
		require.NoError(t, api.Status().Update(ctx, pod))
	}
	ownerRuntimeEventually(t, "real CP Machine readiness", 40*time.Second, func() error {
		_, reconcileErr := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tcp)})
		if reconcileErr != nil {
			return reconcileErr
		}
		for _, m := range machines {
			if e := api.Get(ctx, client.ObjectKeyFromObject(m), m); e != nil {
				return e
			}
			if !m.Status.NodeRef.IsDefined() || !conditions.IsTrue(m, clusterv1.MachineReadyCondition) {
				return fmt.Errorf("Machine %s is not ready", m.Name)
			}
		}
		if e := api.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); e != nil {
			return e
		}
		if !ptr.Deref(cluster.Status.Initialization.InfrastructureProvisioned, false) {
			return fmt.Errorf("infra not initialized")
		}
		return nil
	})
	require.NoError(t, api.Get(ctx, client.ObjectKeyFromObject(tcp), tcp))
	_, e = r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tcp)})
	require.NoError(t, e)
	require.NoError(t, api.Get(ctx, client.ObjectKeyFromObject(tcp), tcp))
	tcp.Spec.ControlPlaneConfig.ControlPlaneConfig.ConfigRevision = "registry-change"
	tcp.Spec.ControlPlaneConfig.ControlPlaneConfig.StrategicPatchSecretRefs = []bootstrapv1.SecretKeyReference{{Name: "registry", Key: "patch"}}
	require.NoError(t, api.Create(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "registry", Namespace: ns}, Data: map[string][]byte{"patch": []byte("apiVersion: v1alpha1\nkind: RegistryAuthConfig\nname: ghcr.io\nusername: fixture\npassword: " + ownerRuntimePrivate + "\n")}}))
	require.NoError(t, api.Update(ctx, tcp))
	secondBefore := machines[1].DeepCopy()
	secondConfigBefore := configs[1].DeepCopy()
	ownerRuntimeEventually(t, "actual owner release and real Apply", 45*time.Second, func() error {
		_, e := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tcp)})
		if e != nil {
			return e
		}
		for _, node := range nodes {
			node.mu.Lock()
			count := node.applies
			node.mu.Unlock()
			if count > 0 {
				return nil
			}
		}
		return fmt.Errorf("waiting for actual Apply")
	})
	require.NoError(t, api.Get(ctx, client.ObjectKeyFromObject(tcp), tcp))
	ownerSecret, plan, e := r.readOwnerPlan(ctx, tcp)
	require.NoError(t, e)
	require.NotNil(t, plan)
	require.Equal(t, op.OwnerReleased, plan.Intent.Phase)
	targetIndex := 0
	if plan.Intent.Machine.UID == machines[1].UID {
		targetIndex = 1
		secondBefore = machines[0].DeepCopy()
		secondConfigBefore = configs[0].DeepCopy()
	}
	target := nodes[targetIndex]
	// Accepted Apply alone must not produce a receipt before active config recovery.
	for range 2 {
		_, e = r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tcp)})
		require.NoError(t, e)
		observedMachine := &clusterv1.Machine{}
		require.NoError(t, api.Get(ctx, client.ObjectKeyFromObject(secondBefore), observedMachine))
		require.Equal(t, secondBefore.Spec, observedMachine.Spec)
		require.NotContains(t, observedMachine.Annotations, clusterv1.UpdateInProgressAnnotation)
		require.Equal(t, secondBefore.UID, observedMachine.UID)
		nodes[1-targetIndex].mu.Lock()
		peerApplies := nodes[1-targetIndex].applies
		nodes[1-targetIndex].mu.Unlock()
		require.Zero(t, peerApplies, "peer cannot Apply before original recovery")
		observedConfig := &bootstrapv1.TalosConfig{}
		require.NoError(t, api.Get(ctx, client.ObjectKeyFromObject(secondConfigBefore), observedConfig))
		require.Equal(t, secondConfigBefore.Spec, observedConfig.Spec)
		require.Equal(t, secondConfigBefore.UID, observedConfig.UID)
		require.NotContains(t, observedConfig.Annotations, clusterv1.UpdateInProgressAnnotation)
		_, _, err := op.ReadJournal(ctx, api, ns, plan.Intent.TalosConfig)
		require.NoError(t, err)
		receipt, err := op.ReadReceipt(ctx, api, ns, plan.Intent.TalosConfig, plan.Intent.TransactionID)
		require.Error(t, err)
		require.Nil(t, receipt)
	}
	require.NotEmpty(t, ownerSecret.UID)
	target.mu.Lock()
	data := bytes.Clone(target.pending)
	target.release = true
	target.mu.Unlock()
	require.NotEmpty(t, data)
	require.NoError(t, target.setActive(ctx, data))
	// The actual hook advertises 30s at Planned/Issued/effect-observed stages;
	// five intervals include final recovery and controller scheduling margin.
	ownerRuntimeEventually(t, "real receipt core cleanup owner consumption", 150*time.Second, func() error {
		_, e := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(tcp)})
		if e != nil {
			return e
		}
		s, p, e := r.readOwnerPlan(ctx, tcp)
		if e != nil {
			return e
		}
		if s != nil && (s.UID == ownerSecret.UID || p.Intent.TransactionID == plan.Intent.TransactionID || p.Intent.Machine.UID == plan.Intent.Machine.UID) {
			return fmt.Errorf("original owner plan not retired")
		}
		original := &clusterv1.Machine{}
		if e := api.Get(ctx, client.ObjectKey{Namespace: ns, Name: plan.Intent.Machine.Name}, original); e != nil {
			return e
		}
		if isUpdatingInPlace(original) {
			return fmt.Errorf("original core flags not cleared")
		}
		originalConfig := &bootstrapv1.TalosConfig{}
		if e := api.Get(ctx, client.ObjectKey{Namespace: ns, Name: plan.Intent.TalosConfig.Name}, originalConfig); e != nil {
			return e
		}
		if isUpdatingObject(originalConfig) || originalConfig.Annotations[op.OwnerPlanNameAnnotation] != "" || originalConfig.Annotations[op.OwnerPlanUIDAnnotation] != "" {
			return fmt.Errorf("original config references not detached")
		}
		originalInfra := &unstructured.Unstructured{}
		originalInfra.SetAPIVersion(plan.Intent.Infrastructure.APIGroup + "/v1beta1")
		originalInfra.SetKind(plan.Intent.Infrastructure.Kind)
		if e := api.Get(ctx, client.ObjectKey{Namespace: ns, Name: plan.Intent.Infrastructure.Name}, originalInfra); e != nil {
			return e
		}
		if isUpdatingObject(originalInfra) {
			return fmt.Errorf("original infra flag not cleared")
		}
		journalSecret, _, e := op.ReadJournal(ctx, api, ns, plan.Intent.TalosConfig)
		if e != nil {
			return e
		}
		receipt, e := op.ReadReceipt(ctx, api, ns, plan.Intent.TalosConfig, plan.Intent.TransactionID)
		if e != nil {
			return e
		}
		var consumed op.CompletionReceipt
		if json.Unmarshal(journalSecret.Data[op.ConsumedReceiptKey], &consumed) != nil || !reflect.DeepEqual(&consumed, receipt) {
			return fmt.Errorf("original receipt not consumed")
		}
		return nil
	})
	journalSecret, journal, e := op.ReadJournal(ctx, api, ns, plan.Intent.TalosConfig)
	require.NoError(t, e)
	receipt, e := op.ReadReceipt(ctx, api, ns, plan.Intent.TalosConfig, plan.Intent.TransactionID)
	require.NoError(t, e)
	require.Equal(t, op.Observed, journal.Phase)
	require.Equal(t, receipt.InputFreshness, plan.Intent.InputFreshness)
	require.NotEmpty(t, journalSecret.Data[op.ConsumedReceiptKey])
	require.NotEmpty(t, journal.OriginalEtcdMembers)
	originalConfig := &bootstrapv1.TalosConfig{}
	require.NoError(t, api.Get(ctx, client.ObjectKey{Namespace: ns, Name: plan.Intent.TalosConfig.Name}, originalConfig))
	currentSecret, currentPlan, err := r.readOwnerPlan(ctx, tcp)
	require.NoError(t, err)
	if currentSecret != nil {
		require.NotEqual(t, ownerSecret.UID, currentSecret.UID)
		require.Equal(t, secondBefore.UID, currentPlan.Intent.Machine.UID)
		require.NotEqual(t, plan.Intent.TransactionID, currentPlan.Intent.TransactionID)
	}
	output := &corev1.Secret{}
	require.NoError(t, api.Get(ctx, client.ObjectKey{Namespace: ns, Name: originalConfig.Status.DataSecretName}, output))
	accepted, e := op.DecodeAcceptedIntent(output.Data[op.AcceptedIntentKey])
	require.NoError(t, e)
	require.Equal(t, plan.Intent.TransactionID, accepted.TransactionID)
	require.NoError(t, api.Get(ctx, client.ObjectKeyFromObject(tcp), tcp))
	require.Equal(t, "v1.35.0", *tcp.Status.Version)
	t.Logf("actual CACPPT owner -> CABPT subprocess renderer/Apply/Observed receipt -> core cleanup -> owner consumption/status; two simulated CP nodes, no three-node rollout claim; logs bytes core=%d CABPT=%d", len(coreLogs.text()), len(bootstrapLogs.text()))
}

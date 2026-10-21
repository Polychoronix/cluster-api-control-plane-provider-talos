// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// Real TLS/REST requests and the real CAPI ClusterCache consume a token-only
// external kubeconfig. Only the management API is envtest; no ambient cluster is used.
func TestExternalKubeconfigClusterCacheTransport(t *testing.T) {
	var denied atomic.Bool
	var requests, nodeRequests atomic.Int64
	token := "local-test-token"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		if req.Header.Get("Authorization") != "Bearer "+token || denied.Load() {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if req.TLS == nil || req.TLS.ServerName != "example.com" || len(req.TLS.PeerCertificates) != 0 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch req.URL.Path {
		case "/":
			fmt.Fprint(w, `{}`)
		case "/api":
			fmt.Fprint(w, `{"kind":"APIVersions","apiVersion":"v1","versions":["v1"]}`)
		case "/apis":
			fmt.Fprint(w, `{"kind":"APIGroupList","apiVersion":"v1","groups":[]}`)
		case "/api/v1":
			fmt.Fprint(w, `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"v1","resources":[{"name":"nodes","singularName":"node","namespaced":false,"kind":"Node","verbs":["get","list","watch"]}]}`)
		case "/api/v1/nodes":
			nodeRequests.Add(1)
			fmt.Fprint(w, `{"kind":"NodeList","apiVersion":"v1","metadata":{"resourceVersion":"1"},"items":[]}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	cert, err := x509.ParseCertificate(server.TLS.Certificates[0].Certificate[0])
	require.NoError(t, err)
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})

	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join(moduleDirectory(t, "sigs.k8s.io/cluster-api"), "core", "config", "crd", "bases")}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil && os.Getenv("KUBEBUILDER_ASSETS") == "" && strings.Contains(err.Error(), "fork/exec") {
		t.Skipf("envtest binaries are not installed: %v", err)
	}
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	ctx, cancel := context.WithCancel(context.Background())
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}})
	require.NoError(t, err)
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	clusterCache, err := clustercache.SetupWithManager(ctx, mgr, clustercache.Options{SecretClient: c, Client: clustercache.ClientOptions{UserAgent: "cacppt-external-smoke", Timeout: time.Second, Cache: clustercache.ClientCacheOptions{DisableFor: []client.Object{&corev1.Node{}}}}}, controller.Options{MaxConcurrentReconciles: 1})
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })

	cluster := &clusterv1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "external-token", Namespace: "default"}, Spec: clusterv1.ClusterSpec{ControlPlaneEndpoint: clusterv1.APIEndpoint{Host: "localhost", Port: 6443}}}
	credentials := clientcmdapi.NewConfig()
	credentials.Clusters["proxy"] = &clientcmdapi.Cluster{Server: server.URL, TLSServerName: "example.com", CertificateAuthorityData: ca}
	credentials.AuthInfos["proxy"] = &clientcmdapi.AuthInfo{Token: token}
	credentials.Contexts["proxy"] = &clientcmdapi.Context{Cluster: "proxy", AuthInfo: "proxy"}
	credentials.CurrentContext = "proxy"
	data, err := clientcmd.Write(*credentials)
	require.NoError(t, err)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cluster.Name + "-kubeconfig", Namespace: cluster.Namespace, Labels: map[string]string{clusterv1.ClusterNameLabel: cluster.Name}, Annotations: map[string]string{"external-owner": "test"}}, Data: map[string][]byte{"value": data}}
	require.NoError(t, c.Create(ctx, secret))
	require.NoError(t, c.Create(ctx, cluster))
	cluster.Status.Initialization.InfrastructureProvisioned = ptr.To(true)
	require.NoError(t, c.Status().Update(ctx, cluster))
	key := client.ObjectKeyFromObject(cluster)
	require.Eventually(t, func() bool { _, err := clusterCache.GetClient(ctx, key); return err == nil }, 15*time.Second, 20*time.Millisecond)

	tcp := &controlplanev1.TalosControlPlane{Spec: controlplanev1.TalosControlPlaneSpec{Kubeconfig: controlplanev1.KubeconfigSpec{Mode: controlplanev1.KubeconfigModeExternal}}}
	r := &TalosControlPlaneReconciler{Client: c, APIReader: c, ClusterCache: clusterCache, Log: ctrl.Log}
	var before corev1.Secret
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(secret), &before))
	var healthyTransition metav1.Time
	for i := range 2 {
		_, err := r.reconcileKubeconfig(ctx, cluster, tcp, nil)
		require.NoError(t, err)
		callsBefore := nodeRequests.Load()
		require.NoError(t, r.updateStatus(ctx, tcp, cluster))
		require.Greater(t, nodeRequests.Load(), callsBefore, "real CAPI cache-backed client issues TLS Node-list request")
		require.True(t, conditions.IsTrue(tcp, string(controlplanev1.AvailableCondition)))
		transition := conditions.Get(tcp, string(controlplanev1.AvailableCondition)).LastTransitionTime
		if i == 0 {
			healthyTransition = transition
		} else {
			require.Equal(t, healthyTransition, transition, "healthy observations retain transition timestamp")
		}
	}
	// Ensure condition timestamps are distinguishable even with second-resolution serialization.
	time.Sleep(time.Until(healthyTransition.Add(time.Second)) + 10*time.Millisecond)
	denied.Store(true)
	require.NoError(t, r.updateStatus(ctx, tcp, cluster))
	require.False(t, conditions.IsTrue(tcp, string(controlplanev1.AvailableCondition)))
	failedTransition := conditions.Get(tcp, string(controlplanev1.AvailableCondition)).LastTransitionTime
	require.True(t, failedTransition.After(healthyTransition.Time))
	time.Sleep(time.Until(failedTransition.Add(time.Second)) + 10*time.Millisecond)
	denied.Store(false)
	require.NoError(t, r.updateStatus(ctx, tcp, cluster))
	require.True(t, conditions.IsTrue(tcp, string(controlplanev1.AvailableCondition)))
	require.True(t, conditions.Get(tcp, string(controlplanev1.AvailableCondition)).LastTransitionTime.After(failedTransition.Time))
	var after corev1.Secret
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(secret), &after))
	require.Equal(t, before, after, "token bytes, metadata, RV and ownership are untouched; no certificate rotation")
	t.Logf("real TLS requests=%d Node lists=%d, token-only auth, dial localhost/SNI example.com, stable healthy timestamp, denial and recovery observed; Secret unchanged", requests.Load(), nodeRequests.Load())
}

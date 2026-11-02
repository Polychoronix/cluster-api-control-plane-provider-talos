// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	cp "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	"github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/encoder"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"
	k8sresource "github.com/siderolabs/talos/pkg/machinery/resources/k8s"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Owner unit tests simulate provisioning, but activation itself uses the actual
// observer against a local mTLS/COSI runtime and a generated private baseline.
func activateOwnerUnitFixture(t *testing.T, api client.Client, cluster *clusterv1.Cluster, tcp *cp.TalosControlPlane, m *clusterv1.Machine, tc *bootstrapv1.TalosConfig, seed *corev1.Secret) {
	t.Helper()
	bundle, err := secrets.NewBundle(secrets.NewClock(), config.TalosVersion1_14)
	require.NoError(t, err)
	input, err := generate.NewInput(cluster.Name, "https://localhost:6443", strings.TrimPrefix(m.Spec.Version, "v"), generate.WithVersionContract(config.TalosVersion1_14), generate.WithSecretsBundle(bundle))
	require.NoError(t, err)
	baseline, err := input.Config(machine.TypeControlPlane)
	require.NoError(t, err)
	data, err := baseline.EncodeBytes(encoder.WithComments(encoder.CommentsDisabled))
	require.NoError(t, err)
	source := &corev1.Secret{}
	require.NoError(t, api.Get(t.Context(), client.ObjectKey{Namespace: m.Namespace, Name: "owner-baseline"}, source))
	source.Data["value"] = data
	require.NoError(t, api.Update(t.Context(), source))
	seed.Data = map[string][]byte{"value": data}
	require.NoError(t, api.Update(t.Context(), seed))

	node := &corev1.Node{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Node"}, ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid"}, Spec: corev1.NodeSpec{ProviderID: "talos://node"}, Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{KubeletVersion: m.Spec.Version, BootID: "initial-boot"}, Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
	node.Labels = map[string]string{"node-role.kubernetes.io/control-plane": ""}
	workload := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api":
			_ = json.NewEncoder(w).Encode(metav1.APIVersions{TypeMeta: metav1.TypeMeta{Kind: "APIVersions", APIVersion: "v1"}, Versions: []string{"v1"}})
		case "/apis":
			_ = json.NewEncoder(w).Encode(metav1.APIGroupList{TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"}})
		case "/api/v1":
			_ = json.NewEncoder(w).Encode(metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: "v1", APIResources: []metav1.APIResource{{Name: "nodes", Kind: "Node", Verbs: []string{"get", "list"}}}})
		case "/api/v1/nodes":
			_ = json.NewEncoder(w).Encode(corev1.NodeList{TypeMeta: metav1.TypeMeta{Kind: "NodeList", APIVersion: "v1"}, Items: []corev1.Node{*node}})
		case "/api/v1/nodes/node":
			_ = json.NewEncoder(w).Encode(node)
		case "/readyz":
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(workload.Close)
	kube := clientcmdapi.NewConfig()
	kube.Clusters["local"] = &clientcmdapi.Cluster{Server: workload.URL}
	kube.AuthInfos["local"] = &clientcmdapi.AuthInfo{}
	kube.Contexts["local"] = &clientcmdapi.Context{Cluster: "local", AuthInfo: "local"}
	kube.CurrentContext = "local"
	kubeData, err := clientcmd.Write(*kube)
	require.NoError(t, err)
	require.NoError(t, api.Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: m.Namespace, Name: cluster.Name + "-kubeconfig"}, Data: map[string][]byte{"value": kubeData}}))
	runtimeNode := ownerRuntimeNewNode()
	runtimeNode.members, runtimeNode.memberID = []string{node.Name}, 1
	require.NoError(t, runtimeNode.setRuntime(t.Context()))
	kubelet := k8sresource.NewKubeletStatus(k8sresource.NamespaceName, "kubelet")
	kubelet.TypedSpec().Image = "ghcr.io/siderolabs/kubelet:" + m.Spec.Version
	require.NoError(t, runtimeNode.put(t.Context(), kubelet))
	require.NoError(t, runtimeNode.setActive(t.Context(), data))
	endpoint, identity := ownerRuntimeStartNode(t, runtimeNode, bundle, cluster.Name)
	require.NoError(t, api.Create(t.Context(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: m.Namespace, Name: cluster.Name + "-talosconfig"}, Data: map[string][]byte{"talosconfig": identity}}))
	m.Spec.ProviderID = node.Spec.ProviderID
	require.NoError(t, api.Update(t.Context(), m))
	m.Status.NodeRef = clusterv1.MachineNodeReference{Name: node.Name}
	require.NoError(t, api.Status().Update(t.Context(), m))
	tcp.Spec.Manage, tcp.Spec.Replicas = true, ptr.To[int32](1)
	tcp.Spec.ControlPlaneConfig.ControlPlaneConfig.TalosEndpoint = &endpoint
	require.NoError(t, api.Update(t.Context(), tcp))
	tc.Spec.TalosEndpoint, tc.Spec.Manage = &endpoint, true
	require.NoError(t, api.Update(t.Context(), tc))
	tc.Status.DataSecretName = seed.Name
	require.NoError(t, api.Status().Update(t.Context(), tc))
	resolved, err := configinput.Resolve(t.Context(), api, tc)
	require.NoError(t, err)
	rendered, err := configinput.Render(resolved, configinput.RenderContext{Machine: m, Cluster: cluster})
	require.NoError(t, err)
	require.NoError(t, runtimeNode.setActive(t.Context(), rendered.Data()))
	authority := tcpManagementAuthority(tcp)
	effective := tc.DeepCopy()
	desired, err := tcp.Spec.EffectiveTalosConfigSpec(m)
	require.NoError(t, err)
	effective.Spec = *desired
	_, err = op.ActivateManagement(t.Context(), api, api, op.ManagementObservationRequest{Authority: authority, Cluster: cluster, Manage: true, Machines: []*clusterv1.Machine{m}, TalosConfigs: []*bootstrapv1.TalosConfig{tc}, EffectiveTalosConfigs: []*bootstrapv1.TalosConfig{effective}})
	require.NoError(t, err)
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	alpha "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1alpha3"
	beta "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestMachineConfigOverrideSchemaEnvtest(t *testing.T) {
	env := &envtest.Environment{CRDDirectoryPaths: []string{filepath.Join("..", "config", "crd", "bases")}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil && os.Getenv("KUBEBUILDER_ASSETS") == "" && strings.Contains(err.Error(), "fork/exec") {
		t.Skipf("envtest assets not installed: %v", err)
	}
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, env.Stop()) })
	scheme := runtime.NewScheme()
	require.NoError(t, beta.AddToScheme(scheme))
	require.NoError(t, alpha.AddToScheme(scheme))
	api, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	shared := cabptv1.TalosConfigSpec{GenerateType: "controlplane", ConfigSecretRef: &cabptv1.SecretKeyReference{Name: "shared-baseline", Key: "value"}, ConfigRevision: "shared-2", StrategicPatchSecretRefs: []cabptv1.SecretKeyReference{{Name: "shared-first", Key: "patch"}, {Name: "shared-second", Key: "patch"}}}
	overrides := []beta.MachineConfigOverride{{Name: "cp-one", UID: "machine-one", Config: &cabptv1.MachineConfigOverlay{ConfigSecretRef: &cabptv1.SecretKeyReference{Name: "node-baseline", Key: "config"}, ConfigRevision: "node-3", StrategicPatches: []string{"node-first", "node-second"}, StrategicPatchSecretRefs: []cabptv1.SecretKeyReference{{Name: "node-private", Key: "patch"}}}, TalosEndpoint: &cabptv1.TalosEndpoint{Address: "proxy:50000", ServerName: "node.example.test", Node: "cp-one"}}}
	tcp := &beta.TalosControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "inputs", Namespace: "default"}, Spec: beta.TalosControlPlaneSpec{Version: "v1.36.0", ControlPlaneConfig: beta.ControlPlaneConfig{ControlPlaneConfig: shared}, MachineConfigOverrides: overrides}}
	tcp.Spec.Management = &cabptv1.ManagementSpec{Mode: cabptv1.ManagementModeObserve, InventoryRevision: "inventory-1", Machines: []cabptv1.MachineIdentityReference{{Name: "cp-one", UID: "machine-one"}, {Name: "cp-two", UID: "machine-two"}}}
	tcp.Spec.MachineTemplate.Spec.InfrastructureRef = clusterv1.ContractVersionedObjectReference{APIGroup: "infrastructure.cluster.x-k8s.io", Kind: "TestMachineTemplate", Name: "unused"}
	require.NoError(t, api.Create(t.Context(), tcp))
	stored := &beta.TalosControlPlane{}
	require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(tcp), stored))
	require.Equal(t, tcp.Spec.ControlPlaneConfig, stored.Spec.ControlPlaneConfig)
	require.Equal(t, overrides, stored.Spec.MachineConfigOverrides)
	require.Equal(t, tcp.Spec.Management, stored.Spec.Management)
	template := &beta.TalosControlPlaneTemplate{ObjectMeta: metav1.ObjectMeta{Name: "inputs-template", Namespace: "default"}, Spec: beta.TalosControlPlaneTemplateSpec{Template: beta.TalosControlPlaneTemplateResource{Spec: beta.TalosControlPlaneTemplateResourceSpec{ControlPlaneConfig: tcp.Spec.ControlPlaneConfig, MachineConfigOverrides: overrides}}}}
	template.Spec.Template.Spec.Management = tcp.Spec.Management.DeepCopy()
	require.NoError(t, api.Create(t.Context(), template))
	storedTemplate := &beta.TalosControlPlaneTemplate{}
	require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(template), storedTemplate))
	require.Equal(t, template.Spec.Template.Spec.ControlPlaneConfig, storedTemplate.Spec.Template.Spec.ControlPlaneConfig)
	require.Equal(t, overrides, storedTemplate.Spec.Template.Spec.MachineConfigOverrides)
	require.Equal(t, template.Spec.Template.Spec.Management, storedTemplate.Spec.Template.Spec.Management)
	spoke := &alpha.TalosControlPlane{}
	require.NoError(t, alpha.Convert_v1beta1_TalosControlPlane_To_v1alpha3_TalosControlPlane(tcp, spoke, nil))
	spoke.Spec.InfrastructureTemplate = corev1.ObjectReference{APIVersion: "infrastructure.cluster.x-k8s.io/v1beta2", Kind: "TestMachineTemplate", Name: "unused"}
	spoke.Name = "inputs-alpha"
	spoke.ResourceVersion = ""
	spoke.UID = ""
	require.NoError(t, api.Create(t.Context(), spoke))
	storedSpoke := &alpha.TalosControlPlane{}
	require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(spoke), storedSpoke))
	require.Equal(t, spoke.Spec.ControlPlaneConfig, storedSpoke.Spec.ControlPlaneConfig)
	require.Equal(t, spoke.Spec.MachineConfigOverrides, storedSpoke.Spec.MachineConfigOverrides)
	require.Equal(t, spoke.Spec.Management, storedSpoke.Spec.Management)
	for _, mutation := range []struct {
		name   string
		change func(*beta.TalosControlPlane)
	}{
		{"duplicate-name", func(cp *beta.TalosControlPlane) {
			cp.Spec.MachineConfigOverrides = append(cp.Spec.MachineConfigOverrides, overrides[0])
		}},
		{"empty-uid", func(cp *beta.TalosControlPlane) { cp.Spec.MachineConfigOverrides[0].UID = "" }},
		{"no-payload", func(cp *beta.TalosControlPlane) {
			cp.Spec.MachineConfigOverrides[0].Config = nil
			cp.Spec.MachineConfigOverrides[0].TalosEndpoint = nil
		}},
		{"shared-conflict", func(cp *beta.TalosControlPlane) {
			cp.Spec.ControlPlaneConfig.ControlPlaneConfig.MachineConfig = &cabptv1.MachineConfigOverlay{}
		}},
		{"unapproved-management", func(cp *beta.TalosControlPlane) { cp.Spec.Management.Mode = cabptv1.ManagementModeManaged }},
		{"duplicate-managed-machine", func(cp *beta.TalosControlPlane) {
			cp.Spec.Management.Machines = append(cp.Spec.Management.Machines, cp.Spec.Management.Machines[0])
		}},
		{"child-consent", func(cp *beta.TalosControlPlane) {
			cp.Spec.ControlPlaneConfig.ControlPlaneConfig.Management = cp.Spec.Management.DeepCopy()
		}},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			invalid := tcp.DeepCopy()
			invalid.Name = mutation.name
			invalid.ResourceVersion, invalid.UID = "", ""
			mutation.change(invalid)
			require.Error(t, api.Create(t.Context(), invalid), "actual schema admission must reject invalid override")
		})
	}
}

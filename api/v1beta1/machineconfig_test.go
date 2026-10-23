// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1beta1

import (
	"testing"

	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

func TestEffectiveTalosConfigSpecPreservesSharedPolicy(t *testing.T) {
	shared := cabptv1.TalosConfigSpec{GenerateType: "controlplane", TalosVersion: "v1.14", ConfigSecretRef: &cabptv1.SecretKeyReference{Name: "base", Key: "value"}, ConfigRevision: "shared-2", ConfigPatches: []cabptv1.ConfigPatches{{Op: "add", Path: "/machine/env/PUBLIC", Value: apiextensionsv1.JSON{Raw: []byte(`"shared"`)}}}, StrategicPatches: []string{"shared-first", "shared-second"}, StrategicPatchSecretRefs: []cabptv1.SecretKeyReference{{Name: "registry", Key: "value"}, {Name: "tailnet", Key: "patch"}}, ImageFactory: &cabptv1.ImageFactorySpec{Extensions: []string{"siderolabs/nvme-cli"}}}
	overlay := &cabptv1.MachineConfigOverlay{ConfigSecretRef: &cabptv1.SecretKeyReference{Name: "cp-one", Key: "config"}, ConfigRevision: "node-3", StrategicPatches: []string{"node-one", "node-two"}, StrategicPatchSecretRefs: []cabptv1.SecretKeyReference{{Name: "node-private", Key: "patch"}}}
	spec := TalosControlPlaneSpec{ControlPlaneConfig: ControlPlaneConfig{ControlPlaneConfig: shared}, MachineConfigOverrides: []MachineConfigOverride{{Name: "cp-one", UID: "uid-one", Config: overlay, TalosEndpoint: &cabptv1.TalosEndpoint{Address: "proxy:50000", ServerName: "cp-one.example.test", Node: "cp-one"}}}}
	machine := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "cp-one", Namespace: "cluster", UID: "uid-one"}}
	got, err := spec.EffectiveTalosConfigSpec(machine)
	require.NoError(t, err)
	expected := shared.DeepCopy()
	expected.MachineConfig = overlay
	expected.TalosEndpoint = spec.MachineConfigOverrides[0].TalosEndpoint
	require.Equal(t, expected, got)
	got.StrategicPatches[0] = "changed"
	got.MachineConfig.StrategicPatches[0] = "changed"
	got.TalosEndpoint.Node = "changed"
	require.Equal(t, "shared-first", shared.StrategicPatches[0])
	require.Equal(t, "node-one", overlay.StrategicPatches[0])
	require.Equal(t, "cp-one", spec.MachineConfigOverrides[0].TalosEndpoint.Node)
	fresh, err := spec.EffectiveTalosConfigSpec(nil)
	require.NoError(t, err)
	require.Equal(t, &shared, fresh, "new Machines receive no existing-node override")
	for _, uid := range []types.UID{"", "replacement"} {
		machine.UID = uid
		_, err := spec.EffectiveTalosConfigSpec(machine)
		require.ErrorContains(t, err, "UID pin")
	}
	machine.Name = "other"
	fresh, err = spec.EffectiveTalosConfigSpec(machine)
	require.NoError(t, err)
	require.Equal(t, &shared, fresh)
}

func TestMachineConfigOverrideValidation(t *testing.T) {
	shared := cabptv1.TalosConfigSpec{GenerateType: "controlplane", MachineConfig: &cabptv1.MachineConfigOverlay{StrategicPatches: []string{"shared-node-policy"}}}
	endpoint := &cabptv1.TalosEndpoint{Address: "proxy:50000", ServerName: "node.example.test"}
	valid := MachineConfigOverride{Name: "cp", UID: "uid", TalosEndpoint: endpoint}
	path := field.NewPath("spec", "machineConfigOverrides")
	require.Empty(t, ValidateMachineConfigOverrides(shared, []MachineConfigOverride{valid}, path), "endpoint-only retains shared MachineConfig")
	conflict := valid
	conflict.Config = &cabptv1.MachineConfigOverlay{}
	require.NotEmpty(t, ValidateMachineConfigOverrides(shared, []MachineConfigOverride{conflict}, path))
	for _, tc := range []struct {
		name    string
		entries []MachineConfigOverride
	}{
		{"duplicate name with different UID", []MachineConfigOverride{valid, {Name: "cp", UID: "replacement", TalosEndpoint: endpoint}}},
		{"empty UID", []MachineConfigOverride{{Name: "cp", TalosEndpoint: endpoint}}},
		{"invalid name", []MachineConfigOverride{{Name: "NOT A NAME", UID: "uid", TalosEndpoint: endpoint}}},
		{"no payload", []MachineConfigOverride{{Name: "cp", UID: "uid"}}},
	} {
		t.Run(tc.name, func(t *testing.T) { require.NotEmpty(t, ValidateMachineConfigOverrides(shared, tc.entries, path)) })
	}
}

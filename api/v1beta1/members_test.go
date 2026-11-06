// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1beta1

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
)

func membersSpec(replicas int32, members ...ControlPlaneMember) TalosControlPlaneSpec {
	return TalosControlPlaneSpec{
		Version:  "v1.35.0",
		Replicas: ptr.To(replicas),
		Manage:   true,
		MachineTemplate: TalosControlPlaneMachineTemplate{
			Spec: TalosControlPlaneMachineTemplateSpec{
				InfrastructureRef: clusterv1.ContractVersionedObjectReference{Name: "cp", Kind: "HetznerBareMetalMachineTemplate", APIGroup: "infrastructure.cluster.x-k8s.io"},
			},
		},
		ControlPlaneConfig: ControlPlaneConfig{
			ControlPlaneConfig: cabptv1.TalosConfigSpec{
				GenerateType:             "controlplane",
				TalosVersion:             "v1.14",
				StrategicPatches:         []string{"shared"},
				StrategicPatchSecretRefs: []cabptv1.SecretKeyReference{{Name: "registry", Key: "patch"}},
				TalosEndpoint:            &cabptv1.TalosEndpoint{Address: "shared:50000", ServerName: "shared.example"},
			},
			Members: members,
		},
	}
}

func member(name string) ControlPlaneMember {
	return ControlPlaneMember{
		Name:                   name,
		InfrastructureSelector: map[string]string{"caph.craci.dev/node": name},
		TalosConfigPatch: &ControlPlaneMemberPatch{
			StrategicPatches:         []string{"patch-" + name},
			StrategicPatchSecretRefs: []cabptv1.SecretKeyReference{{Name: name + "-tailscale", Key: "patch"}},
			TalosEndpoint:            &cabptv1.TalosEndpoint{Address: name + "-talos-access:50000", ServerName: name + ".example"},
		},
	}
}

func machineFor(name, label string) *clusterv1.Machine {
	m := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "cluster"}}
	if label != "" {
		m.Labels = map[string]string{MemberLabel: label}
	}
	return m
}

func TestEffectiveTalosConfigSpecWithoutMembersIsShared(t *testing.T) {
	t.Parallel()

	spec := membersSpec(1)
	got, err := spec.EffectiveTalosConfigSpec(machineFor("kissanpentu-abcde", ""))
	require.NoError(t, err)
	expected := spec.ControlPlaneConfig.ControlPlaneConfig.DeepCopy()
	expected.Manage = true
	require.Equal(t, expected, got)
}

func TestEffectiveTalosConfigSpecAppliesMemberOverlay(t *testing.T) {
	t.Parallel()

	spec := membersSpec(2, member("leijonanpentu"), member("jaguaarinpentu"))
	got, err := spec.EffectiveTalosConfigSpec(machineFor("jaguaarinpentu", "jaguaarinpentu"))
	require.NoError(t, err)
	require.True(t, got.Manage)
	require.Equal(t, []string{"shared", "patch-jaguaarinpentu"}, got.StrategicPatches, "overlay patches merge last")
	require.Equal(t, []cabptv1.SecretKeyReference{{Name: "registry", Key: "patch"}, {Name: "jaguaarinpentu-tailscale", Key: "patch"}}, got.StrategicPatchSecretRefs)
	require.Equal(t, &cabptv1.TalosEndpoint{Address: "jaguaarinpentu-talos-access:50000", ServerName: "jaguaarinpentu.example"}, got.TalosEndpoint, "overlay endpoint replaces the shared one")

	// The result must not alias the declared spec.
	got.StrategicPatches[0] = "mutated"
	got.TalosEndpoint.Address = "mutated"
	require.Equal(t, "shared", spec.ControlPlaneConfig.ControlPlaneConfig.StrategicPatches[0])
	require.Equal(t, "jaguaarinpentu-talos-access:50000", spec.ControlPlaneConfig.Members[1].TalosConfigPatch.TalosEndpoint.Address)
}

func TestEffectiveTalosConfigSpecResolvesMemberByLabelThenName(t *testing.T) {
	t.Parallel()

	spec := membersSpec(1, member("leijonanpentu"))
	byLabel, err := spec.EffectiveTalosConfigSpec(machineFor("renamed", "leijonanpentu"))
	require.NoError(t, err)
	byName, err := spec.EffectiveTalosConfigSpec(machineFor("leijonanpentu", ""))
	require.NoError(t, err)
	require.Equal(t, byLabel, byName)

	_, err = spec.EffectiveTalosConfigSpec(machineFor("stranger", ""))
	require.Error(t, err, "a Machine outside the declared membership has no derivable configuration")
	_, err = spec.EffectiveTalosConfigSpec(nil)
	require.Error(t, err)
}

func TestValidateCreateMembersRules(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		spec    TalosControlPlaneSpec
		wantErr string
	}{
		"replicas below members is allowed":  {spec: membersSpec(1, member("a"), member("b"), member("c"))},
		"replicas equal to members":          {spec: membersSpec(3, member("a"), member("b"), member("c"))},
		"replicas above members is rejected": {spec: membersSpec(4, member("a"), member("b"), member("c")), wantErr: "cannot exceed the 3 declared"},
		"duplicate member name":              {spec: membersSpec(1, member("a"), member("a")), wantErr: "Duplicate value"},
		"member name must be a DNS label":    {spec: membersSpec(1, member("Not_A_Label")), wantErr: "members[0].name"},
		"overlay endpoint is validated": {spec: membersSpec(1, ControlPlaneMember{Name: "a", TalosConfigPatch: &ControlPlaneMemberPatch{
			TalosEndpoint: &cabptv1.TalosEndpoint{Address: "no-port"},
		}}), wantErr: "talosConfigPatch"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			tcp := &TalosControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "cp"}, Spec: tc.spec}
			_, err := tcp.ValidateCreate(context.Background(), tcp)
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.True(t, strings.Contains(err.Error(), tc.wantErr), "error %q should mention %q", err.Error(), tc.wantErr)
		})
	}
}

func TestValidateUpdateMembersImmutableNamesMutableOverlays(t *testing.T) {
	t.Parallel()

	old := &TalosControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "cp"}, Spec: membersSpec(1, member("a"), member("b"))}

	grown := old.DeepCopy()
	grown.Spec.ControlPlaneConfig.Members = append(grown.Spec.ControlPlaneConfig.Members, member("c"))
	grown.Spec.Replicas = ptr.To[int32](3)
	_, err := grown.ValidateUpdate(context.Background(), old, grown)
	require.NoError(t, err, "declaring an additional member and stepping replicas is allowed")

	retuned := old.DeepCopy()
	retuned.Spec.ControlPlaneConfig.Members[0].TalosConfigPatch.StrategicPatches = []string{"new-patch"}
	retuned.Spec.ControlPlaneConfig.Members[0].InfrastructureSelector["extra"] = "label"
	_, err = retuned.ValidateUpdate(context.Background(), old, retuned)
	require.NoError(t, err, "overlays and selectors stay mutable")

	renamed := old.DeepCopy()
	renamed.Spec.ControlPlaneConfig.Members[1].Name = "z"
	_, err = renamed.ValidateUpdate(context.Background(), old, renamed)
	require.Error(t, err)
	require.Contains(t, err.Error(), `member "b" cannot be removed or renamed`)

	dropped := old.DeepCopy()
	dropped.Spec.ControlPlaneConfig.Members = dropped.Spec.ControlPlaneConfig.Members[:1]
	_, err = dropped.ValidateUpdate(context.Background(), old, dropped)
	require.Error(t, err)
}

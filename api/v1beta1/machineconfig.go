// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package v1beta1

import (
	"fmt"

	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// MemberFor returns the declared member a Machine was created for, or nil when the
// control plane declares no members. A labelled Machine whose member is no longer
// declared is an error: its configuration cannot be derived any more.
func (spec *TalosControlPlaneSpec) MemberFor(machine *clusterv1.Machine) (*ControlPlaneMember, error) {
	if len(spec.ControlPlaneConfig.Members) == 0 {
		return nil, nil
	}
	if machine == nil {
		return nil, fmt.Errorf("control plane declares members but no Machine was given")
	}
	name := machine.Labels[MemberLabel]
	if name == "" {
		name = machine.Name
	}
	for i := range spec.ControlPlaneConfig.Members {
		if spec.ControlPlaneConfig.Members[i].Name == name {
			return &spec.ControlPlaneConfig.Members[i], nil
		}
	}
	return nil, fmt.Errorf("Machine %s belongs to undeclared control-plane member %q", machine.Name, name)
}

// EffectiveTalosConfigSpec is the configuration one CAPI-created child receives: the shared
// controlplane spec, the parent's consent, and the member overlay when the control plane
// declares members. Overlay patches and Secret references are appended after the shared
// ones so per-host documents merge last; an overlay talosEndpoint replaces the shared one.
func (spec *TalosControlPlaneSpec) EffectiveTalosConfigSpec(machine *clusterv1.Machine) (*cabptv1.TalosConfigSpec, error) {
	result := spec.ControlPlaneConfig.ControlPlaneConfig.DeepCopy()
	result.Manage = spec.Manage
	member, err := spec.MemberFor(machine)
	if err != nil {
		return nil, err
	}
	if member == nil || member.TalosConfigPatch == nil {
		return result, nil
	}
	patch := member.TalosConfigPatch.DeepCopy()
	result.ConfigPatches = appendCopied(result.ConfigPatches, patch.ConfigPatches)
	result.StrategicPatches = appendCopied(result.StrategicPatches, patch.StrategicPatches)
	result.StrategicPatchSecretRefs = appendCopied(result.StrategicPatchSecretRefs, patch.StrategicPatchSecretRefs)
	if patch.TalosEndpoint != nil {
		endpoint := *patch.TalosEndpoint
		result.TalosEndpoint = &endpoint
	}
	return result, nil
}

// appendCopied appends extra to base into a fresh slice so neither input aliases the result.
func appendCopied[T any](base, extra []T) []T {
	if len(extra) == 0 {
		return base
	}
	out := make([]T, 0, len(base)+len(extra))
	out = append(out, base...)
	return append(out, extra...)
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1beta1

import (
	"fmt"

	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// ValidateMachineConfigOverrides validates references without fetching private input.
func ValidateMachineConfigOverrides(shared cabptv1.TalosConfigSpec, overrides []MachineConfigOverride, path *field.Path) field.ErrorList {
	var errs field.ErrorList
	for i := range overrides {
		override := &overrides[i]
		entry := path.Index(i)
		if len(validation.IsDNS1123Subdomain(override.Name)) != 0 {
			errs = append(errs, field.Invalid(entry.Child("name"), "", "must be a Machine name"))
		}
		if override.UID == "" {
			errs = append(errs, field.Required(entry.Child("uid"), "an existing Machine UID is required"))
		}
		for j := range i {
			if overrides[j].Name == override.Name {
				errs = append(errs, field.Duplicate(entry.Child("name"), override.Name))
				break
			}
		}
		if override.Config == nil && override.TalosEndpoint == nil {
			errs = append(errs, field.Required(entry, "config or talosEndpoint is required"))
		}
		if override.Config != nil && shared.MachineConfig != nil {
			errs = append(errs, field.Forbidden(entry.Child("config"), "cannot coexist with shared machineConfig; put shared policy in root patch fields"))
		}
		effective := shared
		if override.Config != nil {
			effective.MachineConfig = override.Config
		}
		if override.TalosEndpoint != nil {
			effective.TalosEndpoint = override.TalosEndpoint
		}
		errs = append(errs, cabptv1.ValidateConfigInputs(entry, effective)...)
	}
	return errs
}

// EffectiveTalosConfigSpec is the single desired configuration builder for create,
// rollout equality and owner update plans. A nil Machine means shared-only new
// provisioning. A known name without its pinned UID is never an override match.
func (spec *TalosControlPlaneSpec) EffectiveTalosConfigSpec(machine *clusterv1.Machine) (*cabptv1.TalosConfigSpec, error) {
	shared := &spec.ControlPlaneConfig.ControlPlaneConfig
	if errs := ValidateMachineConfigOverrides(*shared, spec.MachineConfigOverrides, field.NewPath("spec", "machineConfigOverrides")); len(errs) != 0 {
		return nil, fmt.Errorf("invalid machine configuration overrides: %s", errs.ToAggregate())
	}
	var matching *MachineConfigOverride
	if machine != nil {
		for i := range spec.MachineConfigOverrides {
			override := &spec.MachineConfigOverrides[i]
			if override.Name != machine.Name {
				continue
			}
			if machine.UID == "" || override.UID != machine.UID {
				return nil, fmt.Errorf("Machine %s does not match its configured UID pin", machine.Name)
			}
			matching = override
			break
		}
	}
	result := shared.DeepCopy()
	if matching != nil {
		if matching.Config != nil {
			result.MachineConfig = matching.Config.DeepCopy()
		}
		if matching.TalosEndpoint != nil {
			result.TalosEndpoint = matching.TalosEndpoint.DeepCopy()
		}
	}
	return result, nil
}

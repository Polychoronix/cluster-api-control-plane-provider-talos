// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// validateMachineConfigInventory runs before any lifecycle or etcd side effect.
// Unassociated entries may remain inert, but a known conflicting UID is fatal.
func validateMachineConfigInventory(tcp *controlplanev1.TalosControlPlane, machines []clusterv1.Machine) error {
	if errs := controlplanev1.ValidateMachineConfigOverrides(tcp.Spec.ControlPlaneConfig.ControlPlaneConfig, tcp.Spec.MachineConfigOverrides, field.NewPath("spec", "machineConfigOverrides")); len(errs) != 0 {
		return fmt.Errorf("invalid machine configuration overrides: %s", errs.ToAggregate())
	}
	for i := range machines {
		machine := &machines[i]
		for j := range tcp.Spec.MachineConfigOverrides {
			override := &tcp.Spec.MachineConfigOverrides[j]
			if override.Name == machine.Name && (machine.UID == "" || override.UID != machine.UID) {
				return fmt.Errorf("Machine %s does not match its configured UID pin", machine.Name)
			}
		}
	}
	return nil
}

// validateLiveMachineConfigPins does not let stale caches or selector changes
// hide a recreated pinned name. Missing names remain inert before association.
func (r *TalosControlPlaneReconciler) validateLiveMachineConfigPins(ctx context.Context, tcp *controlplanev1.TalosControlPlane) error {
	if err := validateMachineConfigInventory(tcp, nil); err != nil {
		return err
	}
	reader := r.APIReader
	if reader == nil {
		return fmt.Errorf("APIReader is required for live Machine UID pin validation")
	}
	for i := range tcp.Spec.MachineConfigOverrides {
		pin := &tcp.Spec.MachineConfigOverrides[i]
		machine := &clusterv1.Machine{}
		if err := reader.Get(ctx, client.ObjectKey{Namespace: tcp.Namespace, Name: pin.Name}, machine); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("failed to read pinned Machine %s: %w", pin.Name, err)
		}
		if machine.UID == "" || machine.UID != pin.UID {
			return fmt.Errorf("Machine %s does not match its configured UID pin", machine.Name)
		}
	}
	return nil
}

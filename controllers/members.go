// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/collections"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

// memberNameOf reports which declared member a Machine occupies: the member label, or the
// Machine name for objects created before the label existed.
func memberNameOf(machine *clusterv1.Machine) string {
	if name := machine.Labels[controlplanev1.MemberLabel]; name != "" {
		return name
	}
	return machine.Name
}

// nextFreeMember returns the first declared member, in declared order, that no Machine
// occupies. A Machine still being deleted keeps its member: the replacement reuses the
// name only once the old Machine is gone, so two Machines never claim one host.
func nextFreeMember(tcp *controlplanev1.TalosControlPlane, machines []clusterv1.Machine) (*controlplanev1.ControlPlaneMember, error) {
	members := tcp.Spec.ControlPlaneConfig.Members
	if len(members) == 0 {
		return nil, nil
	}
	occupied := map[string]bool{}
	for i := range machines {
		occupied[memberNameOf(&machines[i])] = true
	}
	for i := range members {
		if !occupied[members[i].Name] {
			return &members[i], nil
		}
	}
	return nil, fmt.Errorf("all %d declared control-plane members already have a Machine", len(members))
}

// selectMemberForScaleDown picks the occupied member with the highest declared index, so
// scale-down walks the membership in reverse declared order. Machines outside the declared
// membership are removed first: they cannot be reconciled against any member.
func selectMemberForScaleDown(tcp *controlplanev1.TalosControlPlane, machines collections.Machines) *clusterv1.Machine {
	index := map[string]int{}
	for i, member := range tcp.Spec.ControlPlaneConfig.Members {
		index[member.Name] = i
	}
	var victim *clusterv1.Machine
	victimIndex := -1
	for _, machine := range machines.UnsortedList() {
		position, declared := index[memberNameOf(machine)]
		if !declared {
			return machine
		}
		if position > victimIndex || (position == victimIndex && victim != nil && machine.Name < victim.Name) {
			victim, victimIndex = machine, position
		}
	}
	return victim
}

// applyMemberHostSelector merges the member's infrastructureSelector over the cloned
// InfrastructureMachine's spec.hostSelector.matchLabels so the provider claims that host.
func applyMemberHostSelector(infra *unstructured.Unstructured, member *controlplanev1.ControlPlaneMember) error {
	if member == nil || len(member.InfrastructureSelector) == 0 {
		return nil
	}
	labels, _, err := unstructured.NestedStringMap(infra.Object, "spec", "hostSelector", "matchLabels")
	if err != nil {
		return fmt.Errorf("InfrastructureMachine %s spec.hostSelector.matchLabels: %w", infra.GetName(), err)
	}
	if labels == nil {
		labels = map[string]string{}
	}
	for key, value := range member.InfrastructureSelector {
		labels[key] = value
	}
	return unstructured.SetNestedStringMap(infra.Object, labels, "spec", "hostSelector", "matchLabels")
}

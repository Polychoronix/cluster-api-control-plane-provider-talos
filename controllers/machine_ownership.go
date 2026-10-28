// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"

	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/external"
	"sigs.k8s.io/cluster-api/util/annotations"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// readDeclaredControlPlaneInventory resolves an explicit UID-pinned inventory
// without requiring TCP ownership. Observation never creates or associates an
// object; referenced bootstrap and infrastructure ownership must already have
// settled through the normal Machine controller. The shared observation engine
// subsequently freezes child and runtime identities from these live Machines.
func (r *TalosControlPlaneReconciler) readDeclaredControlPlaneInventory(ctx context.Context, cluster *clusterv1.Cluster, declared []*clusterv1.Machine) (clusterv1.MachineList, error) {
	var result clusterv1.MachineList
	if r.APIReader == nil {
		return result, fmt.Errorf("APIReader is required for control-plane inventory observation")
	}
	if cluster == nil || cluster.UID == "" {
		return result, fmt.Errorf("Cluster identity is unavailable")
	}
	if annotations.IsPaused(cluster, cluster) {
		return result, op.ErrManagementPaused
	}
	for i, pin := range declared {
		if pin == nil || pin.Name == "" || pin.UID == "" || pin.Namespace != cluster.Namespace {
			return result, fmt.Errorf("declared Machine identity is incomplete or outside the Cluster namespace")
		}
		for j := range i {
			if declared[j].Name == pin.Name || declared[j].UID == pin.UID {
				return result, fmt.Errorf("declared Machine inventory contains a duplicate identity")
			}
		}
		machine := &clusterv1.Machine{}
		if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(pin), machine); err != nil {
			return result, fmt.Errorf("cannot observe declared Machine %s", pin.Name)
		}
		_, controlPlane := machine.Labels[clusterv1.MachineControlPlaneLabel]
		if machine.UID != pin.UID || machine.Spec.ClusterName != cluster.Name || machine.Labels[clusterv1.ClusterNameLabel] != cluster.Name || !controlPlane || !machine.DeletionTimestamp.IsZero() {
			return result, fmt.Errorf("declared Machine %s identity, Cluster or role changed", pin.Name)
		}
		if _, paused := machine.Annotations[clusterv1.PausedAnnotation]; paused {
			return result, op.ErrManagementPaused
		}
		if machine.Spec.Bootstrap.ConfigRef.APIGroup != bootstrapv1.GroupVersion.Group || machine.Spec.Bootstrap.ConfigRef.Kind != "TalosConfig" {
			return result, fmt.Errorf("declared Machine %s has no TalosConfig association", pin.Name)
		}
		config := &bootstrapv1.TalosConfig{}
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: machine.Namespace, Name: machine.Spec.Bootstrap.ConfigRef.Name}, config); err != nil {
			return result, fmt.Errorf("cannot observe TalosConfig for declared Machine %s", pin.Name)
		}
		if _, paused := config.Annotations[clusterv1.PausedAnnotation]; paused {
			return result, op.ErrManagementPaused
		}
		if config.UID == "" || !config.DeletionTimestamp.IsZero() || !controlledByMachine(config, machine) {
			return result, fmt.Errorf("TalosConfig ownership for declared Machine %s is not settled", pin.Name)
		}
		// The contract resolver reads the actual served infrastructure API. This
		// is lookup only, never the lifecycle clone/owner-reference reconciler.
		infra, err := external.GetObjectFromContractVersionedRef(ctx, r.APIReader, machine.Spec.InfrastructureRef, machine.Namespace)
		if err != nil || infra.GetUID() == "" || !infra.GetDeletionTimestamp().IsZero() || !controlledByMachine(infra, machine) {
			return result, fmt.Errorf("infrastructure ownership for declared Machine %s is not settled", pin.Name)
		}
		if _, paused := infra.GetAnnotations()[clusterv1.PausedAnnotation]; paused {
			return result, op.ErrManagementPaused
		}
		result.Items = append(result.Items, *machine)
	}
	return result, nil
}

func controlledByMachine(object client.Object, machine *clusterv1.Machine) bool {
	owner := metav1.GetControllerOf(object)
	if owner == nil || owner.Kind != "Machine" || owner.Name != machine.Name || owner.UID != machine.UID {
		return false
	}
	gv, err := schema.ParseGroupVersion(owner.APIVersion)
	return err == nil && gv.Group == clusterv1.GroupVersion.Group
}

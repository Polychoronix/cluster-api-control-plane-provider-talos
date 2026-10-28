// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"net"

	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	sharedclient "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/talosclient"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

func talosEndpointForMachine(tcp *controlplanev1.TalosControlPlane, machine *clusterv1.Machine) (cabptv1.TalosEndpoint, error) {
	if machine.Namespace != tcp.Namespace {
		return cabptv1.TalosEndpoint{}, fmt.Errorf("Machine %s is outside the control plane namespace", machine.Name)
	}
	spec, err := tcp.Spec.EffectiveTalosConfigSpec(machine)
	if err != nil {
		return cabptv1.TalosEndpoint{}, err
	}
	if spec.TalosEndpoint != nil {
		return *spec.TalosEndpoint, nil
	}
	// Ordinary provisioning retains address fallback. Never change Node IP policy
	// to reach Talos; an explicit endpoint bypasses these addresses completely.
	for _, kind := range []clusterv1.MachineAddressType{clusterv1.MachineInternalIP, clusterv1.MachineExternalIP} {
		for _, address := range machine.Status.Addresses {
			if address.Type != kind {
				continue
			}
			host := address.Address
			port := "50000"
			if suppliedHost, suppliedPort, err := net.SplitHostPort(address.Address); err == nil {
				host, port = suppliedHost, suppliedPort
			}
			return cabptv1.TalosEndpoint{Address: net.JoinHostPort(host, port), ServerName: host}, nil
		}
	}
	return cabptv1.TalosEndpoint{}, fmt.Errorf("no Talos endpoint or node address for Machine %s", machine.Name)
}

// talosClientForMachine opens exactly one transport/TLS identity and returns its
// routed RPC context. Callers reuse both for the operation and close the client.
func (r *TalosControlPlaneReconciler) talosClientForMachine(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machine *clusterv1.Machine) (*talosclient.Client, context.Context, error) {
	if r.APIReader == nil {
		return nil, nil, fmt.Errorf("APIReader is required for Talos identity lookup")
	}
	endpoint, err := talosEndpointForMachine(tcp, machine)
	if err != nil {
		return nil, nil, err
	}
	clusterName := tcp.Labels[clusterv1.ClusterNameLabel]
	for _, ref := range tcp.OwnerReferences {
		if ref.Kind != "Cluster" {
			continue
		}
		gv, err := schema.ParseGroupVersion(ref.APIVersion)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid Cluster owner reference")
		}
		if gv.Group == clusterv1.GroupVersion.Group {
			clusterName = ref.Name
			break
		}
	}
	if clusterName == "" {
		return nil, nil, fmt.Errorf("failed to determine control plane Cluster name")
	}
	c, err := sharedclient.New(ctx, r.APIReader, types.NamespacedName{Namespace: tcp.Namespace, Name: clusterName}, endpoint)
	if err != nil {
		return nil, nil, err
	}
	return c, sharedclient.WithNode(ctx, endpoint), nil
}

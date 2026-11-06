// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"strings"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	talosclient "github.com/siderolabs/talos/pkg/machinery/client"
	"google.golang.org/grpc"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

// etcdCalls is the subset of the Talos machine API the etcd cleanup and health-check paths
// use. *talosclient.Client satisfies it; tests substitute a fake through
// TalosControlPlaneReconciler.etcdDialer.
type etcdCalls interface {
	ServiceInfo(ctx context.Context, id string, callOptions ...grpc.CallOption) ([]talosclient.ServiceInfo, error)
	ServiceList(ctx context.Context, callOptions ...grpc.CallOption) (*machineapi.ServiceListResponse, error)
	EtcdMemberList(ctx context.Context, req *machineapi.EtcdMemberListRequest, callOptions ...grpc.CallOption) (*machineapi.EtcdMemberListResponse, error)
	EtcdForfeitLeadership(ctx context.Context, req *machineapi.EtcdForfeitLeadershipRequest, callOptions ...grpc.CallOption) (*machineapi.EtcdForfeitLeadershipResponse, error)
	EtcdLeaveCluster(ctx context.Context, req *machineapi.EtcdLeaveClusterRequest, callOptions ...grpc.CallOption) error
	EtcdRemoveMemberByID(ctx context.Context, req *machineapi.EtcdRemoveMemberByIDRequest, callOptions ...grpc.CallOption) error
	Close() error
}

// etcdClientFor returns one Machine's client and its explicit target context.
func (r *TalosControlPlaneReconciler) etcdClientFor(ctx context.Context, tcp *controlplanev1.TalosControlPlane, machine clusterv1.Machine) (etcdCalls, context.Context, error) {
	if r.etcdDialer != nil {
		if _, err := tcp.Spec.EffectiveTalosConfigSpec(&machine); err != nil {
			return nil, nil, err
		}
		c, err := r.etcdDialer(ctx, tcp, machine)
		return c, ctx, err
	}
	return r.talosClientForMachine(ctx, tcp, &machine)
}

// machineHostNames returns every name etcd may report for a machine's member, most
// trustworthy first: the NodeRef name (the kubelet registered under the Talos hostname,
// which is also the etcd member name), then a MachineHostName status address. The address
// is a fallback only: infrastructure providers fill it with their own idea of a hostname
// (CAPH uses the HetznerBareMetalMachine name), and a Machine whose name differs from its
// node (a pre-members `<cluster>-xxxxx` Machine on a host called `tiikerinpentu`) must
// never be mistaken for an etcd member without a Machine. FQDNs are cut to the first label.
func machineHostNames(machine *clusterv1.Machine) []string {
	var names []string
	add := func(name string) {
		name, _, _ = strings.Cut(name, ".")
		if name == "" {
			return
		}
		for _, existing := range names {
			if strings.EqualFold(existing, name) {
				return
			}
		}
		names = append(names, name)
	}
	add(machine.Status.NodeRef.Name)
	for _, address := range machine.Status.Addresses {
		if address.Type == clusterv1.MachineHostName {
			add(address.Address)
		}
	}
	return names
}

// machineIsEtcdMember reports whether an etcd member hostname belongs to the machine.
func machineIsEtcdMember(machine *clusterv1.Machine, memberHostname string) bool {
	for _, name := range machineHostNames(machine) {
		if strings.EqualFold(name, memberHostname) {
			return true
		}
	}
	return false
}

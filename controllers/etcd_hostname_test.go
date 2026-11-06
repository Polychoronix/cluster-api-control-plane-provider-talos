package controllers

import (
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
)

// A Machine whose name differs from its node (a pre-members `<cluster>-xxxxx` Machine on
// a host called tiikerinpentu) carries a MachineHostName address equal to the
// HetznerBareMetalMachine name; the etcd member is named after the node. Matching must
// prefer the NodeRef name so the audit never removes a live member (seen live: the audit
// removed tiikerinpentu's own member through tiikerinpentu).
func TestEtcdMemberMatchingPrefersNodeRefOverHostnameAddress(t *testing.T) {
	legacy := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "kissanpentu-g8fjk"}}
	legacy.Status.NodeRef = clusterv1.MachineNodeReference{Name: "tiikerinpentu"}
	legacy.Status.Addresses = clusterv1.MachineAddresses{{Type: clusterv1.MachineHostName, Address: "kissanpentu-g8fjk"}}
	require.Equal(t, []string{"tiikerinpentu", "kissanpentu-g8fjk"}, machineHostNames(legacy))
	require.True(t, machineIsEtcdMember(legacy, "tiikerinpentu"))
	require.True(t, machineIsEtcdMember(legacy, "TIIKERINPENTU"))
	require.False(t, machineIsEtcdMember(legacy, "jaguaarinpentu"))

	member := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "leijonanpentu"}}
	member.Status.NodeRef = clusterv1.MachineNodeReference{Name: "leijonanpentu.internal"}
	member.Status.Addresses = clusterv1.MachineAddresses{{Type: clusterv1.MachineHostName, Address: "leijonanpentu"}}
	require.Equal(t, []string{"leijonanpentu"}, machineHostNames(member), "FQDN cut, duplicates folded")

	noNode := &clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "fresh"}}
	noNode.Status.Addresses = clusterv1.MachineAddresses{{Type: clusterv1.MachineHostName, Address: "fresh-host"}}
	require.Equal(t, []string{"fresh-host"}, machineHostNames(noNode), "address is the fallback without a NodeRef")

	found, err := matchEtcdMember([]*machineapi.EtcdMember{{Id: 1, Hostname: "leijonanpentu"}, {Id: 2, Hostname: "tiikerinpentu"}}, legacy)
	require.NoError(t, err)
	require.Equal(t, uint64(2), found.Id)
}

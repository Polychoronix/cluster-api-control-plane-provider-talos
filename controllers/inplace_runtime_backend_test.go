// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
// Narrow simulated TLS/COSI protocol setup follows the CABPT runtime test harness.
package controllers

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	cosiapi "github.com/cosi-project/runtime/api/v1alpha1"
	"github.com/cosi-project/runtime/pkg/resource"
	"github.com/cosi-project/runtime/pkg/state"
	"github.com/cosi-project/runtime/pkg/state/impl/inmem"
	"github.com/cosi-project/runtime/pkg/state/impl/namespaced"
	cosiserver "github.com/cosi-project/runtime/pkg/state/protobuf/server"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	configresource "github.com/siderolabs/talos/pkg/machinery/resources/config"
	k8sresource "github.com/siderolabs/talos/pkg/machinery/resources/k8s"
	runtimeresource "github.com/siderolabs/talos/pkg/machinery/resources/runtime"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/emptypb"
)

const ownerRuntimePrivate = "private-owner-runtime-registry-token"
const ownerRuntimeSNI = "owner-talos.example.test"

var ownerRuntimeFailureState func() string

type ownerRuntimeLogs struct {
	sync.Mutex
	bytes.Buffer
}

func (l *ownerRuntimeLogs) Write(p []byte) (int, error) {
	l.Lock()
	defer l.Unlock()
	return l.Buffer.Write(p)
}
func (l *ownerRuntimeLogs) text() string { l.Lock(); defer l.Unlock(); return l.Buffer.String() }
func ownerRuntimePort(t *testing.T) int {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, e)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}
func ownerRuntimeCert(t *testing.T, ca *x509.Certificate, key crypto.Signer, name string, usage x509.ExtKeyUsage) (tls.Certificate, []byte, []byte) {
	t.Helper()
	pub, k, e := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, e)
	template := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
	if usage == x509.ExtKeyUsageServerAuth {
		if ip := net.ParseIP(name); ip != nil {
			template.IPAddresses = []net.IP{ip}
		} else {
			template.DNSNames = []string{name}
		}
	}
	der, e := x509.CreateCertificate(rand.Reader, template, ca, pub, key)
	require.NoError(t, e)
	pk, e := x509.MarshalPKCS8PrivateKey(k)
	require.NoError(t, e)
	cert, keyPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pk})
	pair, e := tls.X509KeyPair(cert, keyPEM)
	require.NoError(t, e)
	return pair, cert, keyPEM
}
func ownerRuntimeCertDir(t *testing.T) (string, []byte) {
	t.Helper()
	pub, key, e := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, e)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, e := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	require.NoError(t, e)
	ca, e := x509.ParseCertificate(der)
	require.NoError(t, e)
	_, crt, pk := ownerRuntimeCert(t, ca, key, "127.0.0.1", x509.ExtKeyUsageServerAuth)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "tls.crt"), crt, 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "tls.key"), pk, 0600))
	return dir, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
func ownerRuntimeProcess(t *testing.T, ctx context.Context, binary string, args []string) *ownerRuntimeLogs {
	t.Helper()
	logs := &ownerRuntimeLogs{}
	cmd := exec.CommandContext(ctx, binary, args...)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "KUBECONFIG=") && !strings.HasPrefix(value, "KUBERNETES_SERVICE_") && !strings.HasPrefix(value, "POD_NAMESPACE=") && !strings.HasPrefix(value, "USE_EXISTING_CLUSTER=") {
			cmd.Env = append(cmd.Env, value)
		}
	}
	cmd.Stdout, cmd.Stderr = logs, logs
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case e := <-done:
			if e != nil {
				t.Errorf("test process failed: %v; %s", e, strings.ReplaceAll(logs.text(), ownerRuntimePrivate, "[redacted]"))
			}
		case <-time.After(20 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Error("test process did not terminate")
		}
		require.NotContains(t, logs.text(), ownerRuntimePrivate)
	})
	return logs
}
func ownerRuntimeEventually(t *testing.T, label string, timeout time.Duration, check func() error) {
	t.Helper()
	var err error
	until := time.Now().Add(timeout)
	for time.Now().Before(until) {
		if err = check(); err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	if ownerRuntimeFailureState != nil {
		t.Fatalf("%s: %v; %s", label, err, ownerRuntimeFailureState())
	}
	t.Fatalf("%s: %v", label, err)
}

type ownerRuntimeNode struct {
	machineapi.UnimplementedMachineServiceServer
	mu            sync.Mutex
	resources     state.State
	members       []string
	memberID      uint64
	pending       []byte
	applies       int
	release       bool
	authenticated bool
}

func ownerRuntimeNewNode() *ownerRuntimeNode {
	return &ownerRuntimeNode{resources: state.WrapCore(namespaced.NewState(inmem.Build))}
}
func (n *ownerRuntimeNode) put(ctx context.Context, r resource.Resource) error {
	existing, e := n.resources.Get(ctx, r.Metadata())
	if state.IsNotFoundError(e) {
		return n.resources.Create(ctx, r)
	}
	if e != nil {
		return e
	}
	*r.Metadata() = *existing.Metadata()
	return n.resources.Update(ctx, r)
}
func (n *ownerRuntimeNode) setActive(ctx context.Context, data []byte) error {
	p, e := configloader.NewFromBytes(data)
	if e != nil {
		return fmt.Errorf("simulated node config invalid")
	}
	return n.put(ctx, configresource.NewMachineConfig(p))
}
func (n *ownerRuntimeNode) setRuntime(ctx context.Context) error {
	boot := runtimeresource.NewBootID()
	boot.TypedSpec().BootID = "initial-boot"
	version := runtimeresource.NewVersion()
	version.TypedSpec().Version = "v1.14.0"
	kubelet := k8sresource.NewKubeletStatus(k8sresource.NamespaceName, "kubelet")
	kubelet.TypedSpec().Image = "ghcr.io/siderolabs/kubelet:v1.35.0"
	for _, r := range []resource.Resource{boot, version, kubelet} {
		if e := n.put(ctx, r); e != nil {
			return e
		}
	}
	return nil
}
func (n *ownerRuntimeNode) Version(ctx context.Context, _ *emptypb.Empty) (*machineapi.VersionResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if p, ok := peer.FromContext(ctx); ok {
		if auth, ok := p.AuthInfo.(credentials.TLSInfo); ok {
			n.authenticated = len(auth.State.VerifiedChains) > 0
		}
	}
	return &machineapi.VersionResponse{Messages: []*machineapi.Version{{Version: &machineapi.VersionInfo{Tag: "v1.14.0"}}}}, nil
}
func (n *ownerRuntimeNode) ApplyConfiguration(ctx context.Context, request *machineapi.ApplyConfigurationRequest) (*machineapi.ApplyConfigurationResponse, error) {
	n.mu.Lock()
	n.applies++
	n.pending = bytes.Clone(request.Data)
	release := n.release
	n.mu.Unlock()
	if release {
		if e := n.setActive(ctx, request.Data); e != nil {
			return nil, e
		}
	}
	return &machineapi.ApplyConfigurationResponse{Messages: []*machineapi.ApplyConfiguration{{Mode: machineapi.ApplyConfigurationRequest_NO_REBOOT}}}, nil
}
func (n *ownerRuntimeNode) ServiceList(context.Context, *emptypb.Empty) (*machineapi.ServiceListResponse, error) {
	return &machineapi.ServiceListResponse{Messages: []*machineapi.ServiceList{{Services: []*machineapi.ServiceInfo{{Id: "etcd", State: "Running", Health: &machineapi.ServiceHealth{Healthy: true}, Events: &machineapi.ServiceEvents{Events: []*machineapi.ServiceEvent{{State: "Running"}}}}}}}}, nil
}
func (n *ownerRuntimeNode) EtcdMemberList(context.Context, *machineapi.EtcdMemberListRequest) (*machineapi.EtcdMemberListResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	members := []*machineapi.EtcdMember{}
	for i, name := range n.members {
		members = append(members, &machineapi.EtcdMember{Id: uint64(i + 1), Hostname: name, PeerUrls: []string{"https://" + name + ":2380"}, ClientUrls: []string{"https://" + name + ":2379"}})
	}
	return &machineapi.EtcdMemberListResponse{Messages: []*machineapi.EtcdMembers{{Members: members}}}, nil
}
func (n *ownerRuntimeNode) EtcdStatus(context.Context, *emptypb.Empty) (*machineapi.EtcdStatusResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return &machineapi.EtcdStatusResponse{Messages: []*machineapi.EtcdStatus{{MemberStatus: &machineapi.EtcdMemberStatus{MemberId: n.memberID, Leader: 1, RaftTerm: 2, RaftAppliedIndex: 8}}}}, nil
}
func (n *ownerRuntimeNode) List(_ *machineapi.ListRequest, stream machineapi.MachineService_ListServer) error {
	return stream.Send(&machineapi.FileInfo{Name: "member"})
}
func (n *ownerRuntimeNode) Bootstrap(context.Context, *machineapi.BootstrapRequest) (*machineapi.BootstrapResponse, error) {
	return &machineapi.BootstrapResponse{}, nil
}
func (n *ownerRuntimeNode) Events(_ *machineapi.EventsRequest, stream machineapi.MachineService_EventsServer) error {
	if e := stream.Send(&machineapi.Event{}); e != nil {
		return e
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}
func ownerRuntimeStartNode(t *testing.T, node *ownerRuntimeNode, bundle *secrets.Bundle, cluster string) (bootstrapv1.TalosEndpoint, []byte) {
	t.Helper()
	ca, e := bundle.Certs.OS.GetCert()
	require.NoError(t, e)
	key, e := bundle.Certs.OS.GetKey()
	require.NoError(t, e)
	signer := key.(crypto.Signer)
	cert, _, _ := ownerRuntimeCert(t, ca, signer, ownerRuntimeSNI, x509.ExtKeyUsageServerAuth)
	_, clientCert, clientKey := ownerRuntimeCert(t, ca, signer, "owner-runtime-client", x509.ExtKeyUsageClientAuth)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, e)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots})), grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if len(md.Get("nodes")) != 0 {
			return nil, fmt.Errorf("unexpected multi-node route")
		}
		return handler(ctx, req)
	}))
	machineapi.RegisterMachineServiceServer(server, node)
	cosiapi.RegisterStateServer(server, cosiserver.NewState(node.resources))
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	identity := []byte(fmt.Sprintf("context: %s\ncontexts:\n  %s:\n    ca: %s\n    crt: %s\n    key: %s\n", cluster, cluster, base64.StdEncoding.EncodeToString(bundle.Certs.OS.Crt), base64.StdEncoding.EncodeToString(clientCert), base64.StdEncoding.EncodeToString(clientKey)))
	return bootstrapv1.TalosEndpoint{Address: listener.Addr().String(), ServerName: ownerRuntimeSNI}, identity
}

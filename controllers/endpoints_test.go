// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	clientconfig "github.com/siderolabs/talos/pkg/machinery/client/config"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type endpointMachineServer struct {
	machineapi.UnimplementedMachineServiceServer
}

func (*endpointMachineServer) ServiceList(context.Context, *emptypb.Empty) (*machineapi.ServiceListResponse, error) {
	return &machineapi.ServiceListResponse{Messages: []*machineapi.ServiceList{{Services: []*machineapi.ServiceInfo{{Id: "etcd", State: "Running", Events: &machineapi.ServiceEvents{Events: []*machineapi.ServiceEvent{{State: "Running"}}}, Health: &machineapi.ServiceHealth{Healthy: true}}}}}}, nil
}
func (*endpointMachineServer) EtcdMemberList(context.Context, *machineapi.EtcdMemberListRequest) (*machineapi.EtcdMemberListResponse, error) {
	return &machineapi.EtcdMemberListResponse{Messages: []*machineapi.EtcdMembers{{Members: []*machineapi.EtcdMember{{Id: 1, Hostname: "cp-one"}, {Id: 2, Hostname: "cp-two"}}}}}, nil
}
func (*endpointMachineServer) EtcdForfeitLeadership(context.Context, *machineapi.EtcdForfeitLeadershipRequest) (*machineapi.EtcdForfeitLeadershipResponse, error) {
	return &machineapi.EtcdForfeitLeadershipResponse{}, nil
}
func (*endpointMachineServer) EtcdLeaveCluster(context.Context, *machineapi.EtcdLeaveClusterRequest) (*machineapi.EtcdLeaveClusterResponse, error) {
	return &machineapi.EtcdLeaveClusterResponse{}, nil
}
func (*endpointMachineServer) EtcdRemoveMemberByID(context.Context, *machineapi.EtcdRemoveMemberByIDRequest) (*machineapi.EtcdRemoveMemberByIDResponse, error) {
	return &machineapi.EtcdRemoveMemberByIDResponse{}, nil
}
func (*endpointMachineServer) Bootstrap(context.Context, *machineapi.BootstrapRequest) (*machineapi.BootstrapResponse, error) {
	return &machineapi.BootstrapResponse{}, nil
}
func (*endpointMachineServer) List(*machineapi.ListRequest, machineapi.MachineService_ListServer) error {
	return nil
}
func (*endpointMachineServer) Events(_ *machineapi.EventsRequest, stream machineapi.MachineService_EventsServer) error {
	if err := stream.Send(&machineapi.Event{}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

type endpointCall struct{ method, target string }

func TestPerMachineEndpointsActualMTLSAndRouting(t *testing.T) {
	caPublic, caKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caPublic, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	issue := func(name string, usage x509.ExtKeyUsage) (tls.Certificate, []byte, []byte) {
		public, key, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		template := &x509.Certificate{SerialNumber: big.NewInt(2), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}, DNSNames: []string{name}}
		der, err := x509.CreateCertificate(rand.Reader, template, caCert, public, caKey)
		require.NoError(t, err)
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		require.NoError(t, err)
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
		pair, err := tls.X509KeyPair(certPEM, keyPEM)
		require.NoError(t, err)
		return pair, certPEM, keyPEM
	}
	_, clientCert, clientKey := issue("imported-management-client", x509.ExtKeyUsageClientAuth)
	identity := &clientconfig.Config{Context: "imported", Contexts: map[string]*clientconfig.Context{"imported": {CA: base64.StdEncoding.EncodeToString(caPEM), Crt: base64.StdEncoding.EncodeToString(clientCert), Key: base64.StdEncoding.EncodeToString(clientKey)}}}
	identityData, err := identity.Bytes()
	require.NoError(t, err)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "cluster-talosconfig", Namespace: "default", UID: "identity-uid"}, Data: map[string][]byte{"talosconfig": identityData}}
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	api := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	tcp := &controlplanev1.TalosControlPlane{ObjectMeta: metav1.ObjectMeta{Name: "cp", Namespace: "default", Labels: map[string]string{clusterv1.ClusterNameLabel: "cluster"}}, Spec: controlplanev1.TalosControlPlaneSpec{ControlPlaneConfig: controlplanev1.ControlPlaneConfig{ControlPlaneConfig: cabptv1.TalosConfigSpec{GenerateType: "controlplane"}}}}
	var calls []endpointCall
	var mu sync.Mutex
	machines := []clusterv1.Machine{{ObjectMeta: metav1.ObjectMeta{Name: "cp-one", Namespace: "default", UID: "one"}}, {ObjectMeta: metav1.ObjectMeta{Name: "cp-two", Namespace: "default", UID: "two"}}}
	for i := range machines {
		machine := &machines[i]
		sni := machine.Name + ".example.test"
		pair, _, _ := issue(sni, x509.ExtKeyUsageServerAuth)
		roots := x509.NewCertPool()
		require.True(t, roots.AppendCertsFromPEM(caPEM))
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{pair}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
		tlsConfig.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			if hello.ServerName != sni {
				return nil, fmt.Errorf("wrong SNI")
			}
			return nil, nil
		}
		record := func(ctx context.Context, method string) {
			md, _ := metadata.FromIncomingContext(ctx)
			if target := strings.Join(md.Get("node"), ","); target != machine.Name {
				t.Errorf("endpoint %s received target %q", machine.Name, target)
			}
			mu.Lock()
			calls = append(calls, endpointCall{method: method, target: strings.Join(md.Get("node"), ",")})
			mu.Unlock()
		}
		server := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)), grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			record(ctx, info.FullMethod)
			return handler(ctx, req)
		}), grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			record(stream.Context(), info.FullMethod)
			return handler(srv, stream)
		}))
		machineapi.RegisterMachineServiceServer(server, &endpointMachineServer{})
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(server.Stop)
		tcp.Spec.MachineConfigOverrides = append(tcp.Spec.MachineConfigOverrides, controlplanev1.MachineConfigOverride{Name: machine.Name, UID: machine.UID, TalosEndpoint: &cabptv1.TalosEndpoint{Address: listener.Addr().String(), ServerName: sni, Node: machine.Name}})
		// Kubernetes node addresses are deliberately unreachable and stay untouched.
		machine.Status.Addresses = []clusterv1.MachineAddress{{Type: clusterv1.MachineInternalIP, Address: "192.0.2.1"}}
		machine.Status.NodeRef = clusterv1.MachineNodeReference{Name: machine.Name}
	}
	r := &TalosControlPlaneReconciler{Client: api, APIReader: api, Log: ctrl.Log}
	require.NoError(t, r.nodesHealthcheck(t.Context(), tcp, machines))
	require.NoError(t, r.etcdHealthcheck(t.Context(), tcp, machines))
	require.NoError(t, r.ensureNodesBooted(t.Context(), tcp, machines))
	require.NoError(t, r.bootstrapCluster(t.Context(), tcp, machines))
	require.NoError(t, r.etcdLeaveFromVictim(t.Context(), tcp, &machines[1]))
	c, rpcCtx, err := r.talosClientForMachine(t.Context(), tcp, &machines[0])
	require.NoError(t, err)
	require.NoError(t, r.forceEtcdLeave(rpcCtx, c, &machineapi.EtcdMember{Id: 2}))
	require.NoError(t, c.Close())
	mu.Lock()
	observed := append([]endpointCall(nil), calls...)
	mu.Unlock()
	require.NotEmpty(t, observed)
	for _, call := range observed {
		require.Contains(t, []string{"cp-one", "cp-two"}, call.target)
	}
	for _, suffix := range []string{"/ServiceList", "/EtcdMemberList", "/Events", "/List", "/Bootstrap", "/EtcdForfeitLeadership", "/EtcdLeaveCluster", "/EtcdRemoveMemberByID"} {
		found := false
		for _, call := range observed {
			if strings.HasSuffix(call.method, suffix) {
				found = true
			}
		}
		require.True(t, found, "actual RPC %s must have reached the mTLS endpoint", suffix)
	}
	stored := &corev1.Secret{}
	require.NoError(t, api.Get(t.Context(), client.ObjectKeyFromObject(secret), stored))
	require.Equal(t, identityData, stored.Data["talosconfig"])
	originalSNI := tcp.Spec.MachineConfigOverrides[0].TalosEndpoint.ServerName
	tcp.Spec.MachineConfigOverrides[0].TalosEndpoint.ServerName = "wrong.example.test"
	badCtx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	badClient, badRPC, err := r.talosClientForMachine(badCtx, tcp, &machines[0])
	require.NoError(t, err)
	_, err = badClient.ServiceList(badRPC)
	require.Error(t, err, "actual RPC must fail wrong certificate name")
	require.NoError(t, badClient.Close())
	cancel()
	tcp.Spec.MachineConfigOverrides[0].TalosEndpoint.ServerName = originalSNI
	mu.Lock()
	require.Len(t, calls, len(observed), "failed TLS must not reach the Talos handler")
	mu.Unlock()
	tcp.Spec.MachineConfigOverrides[0].UID = types.UID("replacement")
	_, _, err = r.talosClientForMachine(t.Context(), tcp, &machines[0])
	require.ErrorContains(t, err, "UID pin")
	t.Logf("%d actual mTLS RPCs through two dial addresses/different SNI targets; imported identity unchanged", len(observed))
}

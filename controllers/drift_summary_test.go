// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

func driftSummaryFixture(t *testing.T, statuses map[string]*metav1.Condition) (*TalosControlPlaneReconciler, *controlplanev1.TalosControlPlane, *clusterv1.MachineList) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, cabptv1.AddToScheme(scheme))
	require.NoError(t, clusterv1.AddToScheme(scheme))
	require.NoError(t, controlplanev1.AddToScheme(scheme))
	var objects []client.Object
	machines := &clusterv1.MachineList{}
	for name, condition := range statuses {
		config := &cabptv1.TalosConfig{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name}}
		if condition != nil {
			config.Status.Conditions = []metav1.Condition{*condition}
		}
		objects = append(objects, config)
		machine := clusterv1.Machine{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name}}
		machine.Spec.Bootstrap.ConfigRef = clusterv1.ContractVersionedObjectReference{APIGroup: cabptv1.GroupVersion.Group, Kind: "TalosConfig", Name: name}
		machines.Items = append(machines.Items, machine)
	}
	reconciler := &TalosControlPlaneReconciler{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(), Log: ctrl.Log.WithName("test")}
	return reconciler, &controlplanev1.TalosControlPlane{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "tcp"}}, machines
}

func TestConfigDriftSummary(t *testing.T) {
	drifted := &metav1.Condition{Type: configDriftConditionType, Status: metav1.ConditionTrue, Reason: "ConfigDrifted", Message: "live configuration differs from the TalosConfig render in: v1alpha1/KubeNodeConfig"}
	clean := &metav1.Condition{Type: configDriftConditionType, Status: metav1.ConditionFalse, Reason: "NoDrift"}
	pending := &metav1.Condition{Type: configDriftConditionType, Status: metav1.ConditionUnknown, Reason: "ObservationPending"}

	r, tcp, machines := driftSummaryFixture(t, map[string]*metav1.Condition{"cp-1": clean, "cp-2": drifted})
	r.reconcileConfigDriftSummary(context.Background(), tcp, machines)
	summary := conditions.Get(tcp, configDriftConditionType)
	require.NotNil(t, summary)
	require.Equal(t, metav1.ConditionTrue, summary.Status)
	require.Equal(t, "ConfigDrifted", summary.Reason)
	require.Equal(t, "cp-2: live configuration differs from the TalosConfig render in: v1alpha1/KubeNodeConfig", summary.Message)

	r, tcp, machines = driftSummaryFixture(t, map[string]*metav1.Condition{"cp-1": clean, "cp-2": clean})
	r.reconcileConfigDriftSummary(context.Background(), tcp, machines)
	summary = conditions.Get(tcp, configDriftConditionType)
	require.Equal(t, metav1.ConditionFalse, summary.Status)
	require.Equal(t, "NoDrift", summary.Reason)

	r, tcp, machines = driftSummaryFixture(t, map[string]*metav1.Condition{"cp-1": clean, "cp-2": pending})
	r.reconcileConfigDriftSummary(context.Background(), tcp, machines)
	summary = conditions.Get(tcp, configDriftConditionType)
	require.Equal(t, metav1.ConditionUnknown, summary.Status)
	require.Contains(t, summary.Message, "cp-2")

	r, tcp, machines = driftSummaryFixture(t, map[string]*metav1.Condition{"cp-1": nil})
	r.reconcileConfigDriftSummary(context.Background(), tcp, machines)
	summary = conditions.Get(tcp, configDriftConditionType)
	require.Equal(t, metav1.ConditionUnknown, summary.Status)
	require.Equal(t, "ObservationPending", summary.Reason)
}

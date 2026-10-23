// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	"github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/configinput"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	cp "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"reflect"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimev1 "sigs.k8s.io/cluster-api/api/runtime/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
	"time"
)

func runtimeConditionState(conditions []metav1.Condition) string {
	var states []string
	for _, c := range conditions {
		states = append(states, c.Type+"="+string(c.Status)+"/"+c.Reason)
	}
	return strings.Join(states, ",")
}
func ownerRuntimeSnapshot(_ context.Context, api client.Client, namespace string, core, bootstrap *ownerRuntimeLogs) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var result strings.Builder
	clusters := &clusterv1.ClusterList{}
	if e := api.List(ctx, clusters, client.InNamespace(namespace)); e != nil {
		fmt.Fprintf(&result, "Cluster read unavailable: %T\n", e)
	}
	for i := range clusters.Items {
		c := &clusters.Items[i]
		fmt.Fprintf(&result, "Cluster %s UID=%s initialization=%v endpoint=%v conditions=%s\n", c.Name, c.UID, c.Status.Initialization, c.Spec.ControlPlaneEndpoint, runtimeConditionState(c.Status.Conditions))
	}
	owners := &cp.TalosControlPlaneList{}
	_ = api.List(ctx, owners, client.InNamespace(namespace))
	for i := range owners.Items {
		c := &owners.Items[i]
		fmt.Fprintf(&result, "TCP %s UID=%s initialization=%v conditions=%s\n", c.Name, c.UID, c.Status.Initialization, runtimeConditionState(c.Status.Conditions))
		s, p, e := (&TalosControlPlaneReconciler{APIReader: api}).readOwnerPlan(ctx, c)
		fmt.Fprintf(&result, "Owner plan present=%t readOK=%t", s != nil, e == nil)
		if p != nil {
			fmt.Fprintf(&result, " phase=%s transaction=%s", p.Intent.Phase, p.Intent.TransactionID)
		}
		fmt.Fprintln(&result)
	}
	machines := &clusterv1.MachineList{}
	_ = api.List(ctx, machines, client.InNamespace(namespace))
	for i := range machines.Items {
		m := &machines.Items[i]
		fmt.Fprintf(&result, "Machine %s UID=%s providerID=%s NodeRef=%s init=%v conditions=%s pending=%q updateMarked=%t\n", m.Name, m.UID, m.Spec.ProviderID, m.Status.NodeRef.Name, m.Status.Initialization, runtimeConditionState(m.Status.Conditions), m.Annotations[runtimev1.PendingHooksAnnotation], isUpdatingObject(m))
		config := &bootstrapv1.TalosConfig{}
		_ = api.Get(ctx, client.ObjectKey{Namespace: namespace, Name: m.Spec.Bootstrap.ConfigRef.Name}, config)
		if len(clusters.Items) > 0 && len(owners.Items) > 0 {
			fmt.Fprintln(&result, ownerRuntimeLineageState(ctx, api, m, config, &owners.Items[0], &clusters.Items[0]))
		}
		journalSecret, journal, e := op.ReadJournal(ctx, api, namespace, ownerIdentity(bootstrapv1.GroupVersion.Group, "TalosConfig", config))
		fmt.Fprintf(&result, "Journal readOK=%t", e == nil)
		if e == nil {
			fmt.Fprintf(&result, " phase=%s receiptPresent=%t consumedPresent=%t", journal.Phase, len(journalSecret.Data[op.ReceiptKey]) > 0, len(journalSecret.Data[op.ConsumedReceiptKey]) > 0)
			for _, step := range journal.Steps {
				fmt.Fprintf(&result, " step=%s/%s/%s", step.Kind, step.Phase, step.ApplyOutcome)
			}
			output := &corev1.Secret{}
			_ = api.Get(ctx, client.ObjectKey{Namespace: namespace, Name: config.Status.DataSecretName}, output)
			fmt.Fprintf(&result, " outputUID=%s valueEqual=%t freshnessEqual=%t", output.UID, bytes.Equal(output.Data["value"], journal.Configuration), bytes.Equal(output.Data[configinput.FreshnessKey], journal.InputFreshness))
			inputs, resolveErr := configinput.Resolve(ctx, api, config)
			fmt.Fprintf(&result, " sourcesResolveOK=%t", resolveErr == nil)
			if resolveErr == nil {
				fmt.Fprintf(&result, " sourcesEqual=%t", reflect.DeepEqual(inputs.Sources(), journal.Inputs))
			}
			var receipt op.CompletionReceipt
			fmt.Fprintf(&result, " receiptDecoded=%t", json.Unmarshal(journalSecret.Data[op.ReceiptKey], &receipt) == nil)
		}
		fmt.Fprintln(&result)
	}
	for _, logs := range []struct {
		name string
		logs *ownerRuntimeLogs
	}{{"core", core}, {"CABPT", bootstrap}} {
		if logs.logs != nil {
			fmt.Fprintf(&result, "%s logs=%s\n", logs.name, strings.ReplaceAll(logs.logs.text(), ownerRuntimePrivate, "[redacted]"))
		}
	}
	return result.String()
}

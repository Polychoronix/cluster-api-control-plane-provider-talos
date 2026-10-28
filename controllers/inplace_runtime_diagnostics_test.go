// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package controllers

import (
	"context"
	"fmt"
	bootstrapv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
	cp "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
)

// Identity-only state: never configuration, Secret data or parser messages.
func ownerRuntimeLineageState(ctx context.Context, api client.Client, machine *clusterv1.Machine, config *bootstrapv1.TalosConfig, tcp *cp.TalosControlPlane, cluster *clusterv1.Cluster) string {
	m := &clusterv1.Machine{}
	c := &bootstrapv1.TalosConfig{}
	owner := &cp.TalosControlPlane{}
	cl := &clusterv1.Cluster{}
	_ = api.Get(ctx, client.ObjectKeyFromObject(machine), m)
	_ = api.Get(ctx, client.ObjectKeyFromObject(config), c)
	_ = api.Get(ctx, client.ObjectKeyFromObject(tcp), owner)
	_ = api.Get(ctx, client.ObjectKeyFromObject(cluster), cl)
	var conditionStates []string
	for _, condition := range c.Status.Conditions {
		conditionStates = append(conditionStates, condition.Type+"="+string(condition.Status)+"/"+condition.Reason)
	}
	infra := &unstructured.Unstructured{}
	infra.SetAPIVersion(m.Spec.InfrastructureRef.APIGroup + "/v1beta1")
	infra.SetKind(m.Spec.InfrastructureRef.Kind)
	_ = api.Get(ctx, client.ObjectKey{Namespace: m.Namespace, Name: m.Spec.InfrastructureRef.Name}, infra)
	return fmt.Sprintf("TC %s UID=%s owners=%v initialTCP=%s/%s generation=%d observed=%d conditions=%s; Machine UID=%s owners=%v bootstrapRef=%v seedPresent=%t NodeRef=%s; Infra UID=%s owners=%v cloned=%s/%s; TCP UID=%s; Cluster UID=%s initialization=%v endpoint=%v", c.Name, c.UID, c.OwnerReferences, c.Annotations[op.InitialControlPlaneNameAnnotation], c.Annotations[op.InitialControlPlaneUIDAnnotation], c.Generation, c.Status.ObservedGeneration, strings.Join(conditionStates, ","), m.UID, m.OwnerReferences, m.Spec.Bootstrap.ConfigRef, m.Spec.Bootstrap.DataSecretName != nil, m.Status.NodeRef.Name, infra.GetUID(), infra.GetOwnerReferences(), infra.GetAnnotations()[clusterv1.TemplateClonedFromNameAnnotation], infra.GetAnnotations()[clusterv1.TemplateClonedFromGroupKindAnnotation], owner.UID, cl.UID, cl.Status.Initialization, cl.Spec.ControlPlaneEndpoint)
}

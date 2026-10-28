// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"strings"

	"github.com/coreos/go-semver/semver"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	k8sresources "github.com/siderolabs/talos/pkg/machinery/resources/k8s"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// prepareVersionObservation discards versions written by the old spec-based
// observer. Presence of this condition records that the new observer owns the
// value: all its failure paths preserve only a previously observed value or nil.
func prepareVersionObservation(tcp *controlplanev1.TalosControlPlane) {
	if conditions.Get(tcp, controlplanev1.VersionObservedCondition) == nil {
		tcp.Status.Version = nil
		conditions.Set(tcp, metav1.Condition{Type: controlplanev1.VersionObservedCondition, Status: metav1.ConditionFalse, Reason: "NotObserved", Message: "Control-plane API-server versions have not been observed"})
	}
}

func versionObservationFailed(tcp *controlplanev1.TalosControlPlane, reason, message string) {
	conditions.Set(tcp, metav1.Condition{Type: controlplanev1.VersionObservedCondition, Status: metav1.ConditionFalse, Reason: reason, Message: message})
}

// observeControlPlaneVersion uses the existing auth-proxy ClusterCache's live
// client. Node association and current running container statuses must cover all
// represented CP instances; a single load-balanced /version is never consulted.
func (r *TalosControlPlaneReconciler) observeControlPlaneVersion(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster, machines []clusterv1.Machine) {
	prepareVersionObservation(tcp)
	if r.ClusterCache == nil {
		versionObservationFailed(tcp, "InspectionFailed", "Workload API reader is unavailable")
		return
	}
	reader, err := r.ClusterCache.GetUncachedClient(ctx, client.ObjectKeyFromObject(cluster))
	if err != nil {
		versionObservationFailed(tcp, "InspectionFailed", "Workload API reader is unavailable")
		return
	}
	var nodes corev1.NodeList
	if err := reader.List(ctx, &nodes); err != nil {
		versionObservationFailed(tcp, "InspectionFailed", "Failed to observe control-plane Node associations")
		return
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(metav1.NamespaceSystem), client.MatchingLabels{"component": k8sresources.APIServerID}); err != nil {
		versionObservationFailed(tcp, "InspectionFailed", "Failed to observe running control-plane API-server Pods")
		return
	}
	version, err := minimumObservedAPIServerVersion(machines, nodes.Items, pods.Items)
	if err != nil {
		versionObservationFailed(tcp, "ObservationIncomplete", err.Error())
		return
	}
	for i := range machines {
		machine := &machines[i]
		if !machine.DeletionTimestamp.IsZero() {
			continue
		}
		updating := conditions.Get(machine, clusterv1.MachineUpdatingCondition)
		if isUpdatingInPlace(machine) || (updating != nil && updating.Status == metav1.ConditionTrue) {
			versionObservationFailed(tcp, "Updating", "API-server versions are observed but an in-place update has not completed")
			return
		}
	}
	tcp.Status.Version = &version
	conditions.Set(tcp, metav1.Condition{Type: controlplanev1.VersionObservedCondition, Status: metav1.ConditionTrue, Reason: "Observed", Message: "All associated control-plane API-server versions are observed"})
}

func minimumObservedAPIServerVersion(machines []clusterv1.Machine, nodes []corev1.Node, pods []corev1.Pod) (string, error) {
	var minimum *semver.Version
	var count int
	for i := range machines {
		machine := &machines[i]
		if !machine.DeletionTimestamp.IsZero() {
			continue
		}
		count++
		if machine.Status.NodeRef.Name == "" {
			return "", fmt.Errorf("Machine %s has no observed Node association", machine.Name)
		}
		for j := range i {
			if machines[j].DeletionTimestamp.IsZero() && machines[j].Status.NodeRef.Name == machine.Status.NodeRef.Name {
				return "", fmt.Errorf("Machine %s has a duplicate Node association", machine.Name)
			}
		}
		var node *corev1.Node
		for j := range nodes {
			if nodes[j].Name == machine.Status.NodeRef.Name {
				if node != nil {
					return "", fmt.Errorf("Machine %s has ambiguous Node observations", machine.Name)
				}
				node = &nodes[j]
			}
		}
		if node == nil || (machine.Spec.ProviderID != "" && machine.Spec.ProviderID != node.Spec.ProviderID) {
			return "", fmt.Errorf("Machine %s Node association cannot be verified", machine.Name)
		}
		if _, cp := node.Labels["node-role.kubernetes.io/control-plane"]; !cp {
			return "", fmt.Errorf("Machine %s Node is not a control-plane Node", machine.Name)
		}
		var pod *corev1.Pod
		for j := range pods {
			candidate := &pods[j]
			if candidate.Spec.NodeName != node.Name {
				continue
			}
			if pod != nil {
				return "", fmt.Errorf("Machine %s has duplicate API-server Pod observations", machine.Name)
			}
			pod = candidate
		}
		if pod == nil || !pod.DeletionTimestamp.IsZero() || pod.Status.Phase != corev1.PodRunning {
			return "", fmt.Errorf("Machine %s has no running API-server Pod observation", machine.Name)
		}
		var container *corev1.ContainerStatus
		for j := range pod.Status.ContainerStatuses {
			candidate := &pod.Status.ContainerStatuses[j]
			if candidate.Name != k8sresources.APIServerID {
				continue
			}
			if container != nil {
				return "", fmt.Errorf("Machine %s has duplicate API-server container observations", machine.Name)
			}
			container = candidate
		}
		if container == nil || container.State.Running == nil || !container.Ready {
			return "", fmt.Errorf("Machine %s API-server container is not running and ready", machine.Name)
		}
		version, err := observedImageVersion(container.Image)
		if err != nil {
			return "", fmt.Errorf("Machine %s API-server image version is not observable", machine.Name)
		}
		if minimum == nil || version.LessThan(*minimum) {
			minimum = version
		}
	}
	if count == 0 {
		return "", fmt.Errorf("No control-plane Machines are available for version observation")
	}
	return "v" + minimum.String(), nil
}

func observedImageVersion(image string) (*semver.Version, error) {
	if i := strings.IndexByte(image, '@'); i >= 0 {
		image = image[:i]
	}
	colon := strings.LastIndexByte(image, ':')
	if colon <= strings.LastIndexByte(image, '/') {
		return nil, fmt.Errorf("no version tag")
	}
	tag := strings.TrimPrefix(image[colon+1:], "v")
	version, err := semver.NewVersion(tag)
	if err != nil {
		return nil, fmt.Errorf("invalid version tag")
	}
	return version, nil
}

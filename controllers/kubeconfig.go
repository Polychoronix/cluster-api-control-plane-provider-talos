// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"net/url"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/cluster-api/util/secret"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *TalosControlPlaneReconciler) reconcileExternalKubeconfig(ctx context.Context, cluster *clusterv1.Cluster, tcp *controlplanev1.TalosControlPlane) (ctrl.Result, error) {
	key := client.ObjectKeyFromObject(cluster)
	config, err := secret.GetFromNamespacedName(ctx, r.APIReader, key, secret.Kubeconfig)
	if apierrors.IsNotFound(err) {
		return externalKubeconfigFailure(tcp, "ExternalKubeconfigMissing", "Standard externally managed kubeconfig Secret is missing")
	}
	if err != nil {
		return externalKubeconfigFailure(tcp, "ExternalKubeconfigReadFailed", "Cannot read externally managed kubeconfig Secret")
	}
	if config.Labels[clusterv1.ClusterNameLabel] != cluster.Name {
		return externalKubeconfigFailure(tcp, "ExternalKubeconfigInvalid", "External kubeconfig Secret must carry the matching cluster.x-k8s.io/cluster-name label for the Cluster cache")
	}
	if !validExternalKubeconfig(config) {
		return externalKubeconfigFailure(tcp, "ExternalKubeconfigInvalid", "External kubeconfig value or current context is invalid")
	}
	conditions.Set(tcp, metav1.Condition{Type: controlplanev1.KubeconfigAvailableCondition, Status: metav1.ConditionTrue, Reason: "ExternalKubeconfigValid"})
	return ctrl.Result{}, nil
}

// Validate only in memory: do not execute auth plugins, resolve file credentials,
// or wrap parser errors, which can quote private kubeconfig content.
func validExternalKubeconfig(secret *corev1.Secret) bool {
	config, err := clientcmd.Load(secret.Data["value"])
	if err != nil || config.CurrentContext == "" || clientcmd.Validate(*config) != nil {
		return false
	}
	current := config.Contexts[config.CurrentContext]
	if current == nil || config.Clusters[current.Cluster] == nil {
		return false
	}
	endpoint, err := url.Parse(config.Clusters[current.Cluster].Server)
	return err == nil && endpoint.Scheme == "https" && endpoint.Hostname() != ""
}

func externalKubeconfigFailure(tcp *controlplanev1.TalosControlPlane, reason, message string) (ctrl.Result, error) {
	conditions.Set(tcp, metav1.Condition{Type: controlplanev1.KubeconfigAvailableCondition, Status: metav1.ConditionFalse, Reason: reason, Message: message})
	return ctrl.Result{RequeueAfter: requeueDuration}, fmt.Errorf("%s", message)
}

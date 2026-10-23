// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// triggerInPlaceUpdate freezes the CAN-approved owner plan before any object
// mutation. Subsequent reconciles recover this exact plan, never newest intent.
func (r *TalosControlPlaneReconciler) triggerInPlaceUpdate(ctx context.Context, tcp *controlplanev1.TalosControlPlane, cluster *clusterv1.Cluster, update *inPlacePlan) error {
	secret, plan, err := r.createOwnerPlan(ctx, tcp, cluster, update)
	if err != nil {
		return err
	}
	_, err = r.resumeOwnerPlan(ctx, tcp, cluster, secret, plan)
	return err
}

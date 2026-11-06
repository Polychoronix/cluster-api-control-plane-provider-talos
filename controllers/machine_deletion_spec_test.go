// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	controlplanev1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

// A TalosControlPlane without deletion settings must render the deletion spec core's Machine
// webhook stores (NodeDeletionTimeoutSeconds defaulted to 10s); otherwise the template sync
// rewrites it on every reconcile and the in-memory Machine never matches the live one.
func TestMachineDeletionSpecMatchesWebhookDefault(t *testing.T) {
	live := clusterv1.MachineDeletionSpec{NodeDeletionTimeoutSeconds: ptr.To[int32](10)}
	require.Equal(t, live, buildMachineDeletionSpec(controlplanev1.TalosControlPlaneMachineTemplateDeletionSpec{}))

	explicit := buildMachineDeletionSpec(controlplanev1.TalosControlPlaneMachineTemplateDeletionSpec{NodeDeletionTimeoutSeconds: ptr.To[int32](50), NodeDrainTimeoutSeconds: ptr.To[int32](30)})
	require.Equal(t, int32(50), *explicit.NodeDeletionTimeoutSeconds)
	require.Equal(t, int32(30), *explicit.NodeDrainTimeoutSeconds)
	require.Nil(t, explicit.NodeVolumeDetachTimeoutSeconds)
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package v1beta1

import (
	cabptv1 "github.com/siderolabs/cluster-api-bootstrap-provider-talos/api/v1beta1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

// EffectiveTalosConfigSpec propagates the single parent consent to every CAPI-created child.
func (spec *TalosControlPlaneSpec) EffectiveTalosConfigSpec(_ *clusterv1.Machine) (*cabptv1.TalosConfigSpec, error) {
	result := spec.ControlPlaneConfig.ControlPlaneConfig.DeepCopy()
	result.Manage = spec.Manage
	return result, nil
}

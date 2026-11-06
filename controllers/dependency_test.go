// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	op "github.com/siderolabs/cluster-api-bootstrap-provider-talos/pkg/updateoperation"
)

// The activation policy, baseline comparison and observation live in CABPT's
// pkg/updateoperation and run inside this controller (management.go calls
// op.ObserveManagement). A CABPT fix there reaches a TalosControlPlane only through the
// vendored module version, never through the CABPT image, so the vendored version is
// pinned explicitly in CABPT_VERSION and must match go.mod (require and replace).
func TestVendoredCABPTVersionMatchesExpected(t *testing.T) {
	expected, err := os.ReadFile("../CABPT_VERSION")
	require.NoError(t, err)
	want := strings.TrimSpace(string(expected))
	require.NotEmpty(t, want)

	mod, err := os.ReadFile("../go.mod")
	require.NoError(t, err)
	require.Regexp(t, regexp.MustCompile(`(?m)^\s*github\.com/siderolabs/cluster-api-bootstrap-provider-talos `+regexp.QuoteMeta(want)+`$`), string(mod), "go.mod require must pin CABPT %s", want)
	require.Regexp(t, regexp.MustCompile(`(?m)^replace github\.com/siderolabs/cluster-api-bootstrap-provider-talos => github\.com/Polychoronix/cluster-api-bootstrap-provider-talos `+regexp.QuoteMeta(want)+`$`), string(mod), "go.mod replace must pin CABPT %s", want)
}

// A successful activation names what the in-place update still has to converge, with
// document identities only.
func TestActivatedMembersMessageNamesDriftAndCurrentBaselines(t *testing.T) {
	require.Equal(t, "", activatedMembersMessage(op.ManagementObservationSummary{}))
	summary := op.ManagementObservationSummary{
		Drifted:          map[string][]string{"leijonanpentu": {"v1alpha1/KubeNodeConfig", "v1alpha1/RawVolumeConfig/ceph-data-osd"}, "kissanpentu-g8fjk": {"v1alpha1/KubeNodeConfig"}},
		CurrentBaselines: []string{"jaguaarinpentu"},
	}
	require.Equal(t, "converging in place: kissanpentu-g8fjk drifted in v1alpha1/KubeNodeConfig; leijonanpentu drifted in v1alpha1/KubeNodeConfig, v1alpha1/RawVolumeConfig/ceph-data-osd; jaguaarinpentu on its current TalosConfig render", activatedMembersMessage(summary))
	long := op.ManagementObservationSummary{Drifted: map[string][]string{"m": {strings.Repeat("v1alpha1/KubeNodeConfig,", 60)}}}
	require.Len(t, activatedMembersMessage(long), 512)
}

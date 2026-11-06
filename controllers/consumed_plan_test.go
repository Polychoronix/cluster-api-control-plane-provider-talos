// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

// The owner plan's last step deletes its Secret; envtest and the fake client do not
// enforce RBAC, so the shipped role is checked directly.
func TestManagerRoleMayDeleteOwnerPlanSecrets(t *testing.T) {
	data, err := os.ReadFile("../config/rbac/role.yaml")
	require.NoError(t, err)
	role := &rbacv1.ClusterRole{}
	require.NoError(t, yaml.Unmarshal(data, role))
	for _, rule := range role.Rules {
		if slices.Contains(rule.APIGroups, "") && slices.Contains(rule.Resources, "secrets") {
			require.Contains(t, rule.Verbs, "delete")
			return
		}
	}
	t.Fatal("no rule for core secrets")
}

type forbiddenOwnerDelete struct {
	client.Client
}

func (c forbiddenOwnerDelete) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	if _, secret := obj.(*corev1.Secret); secret {
		return apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, obj.GetName(), nil)
	}
	return c.Client.Delete(ctx, obj, opts...)
}

// A Consumed plan whose Secret cannot be deleted says why and is retried; once the
// delete is allowed the next resume finishes, and a plan already gone is success.
func TestOwnerConsumedPlanDeletionForbiddenIsRetried(t *testing.T) {
	f := releasedOwner(t)
	s, p, e := f.r.readOwnerPlan(t.Context(), f.tcp)
	require.NoError(t, e)
	ownerReceipt(t, f, s, p)
	cleanupCoreFlags(t, f)
	api := f.r.Client
	f.r.Client = forbiddenOwnerDelete{Client: api}
	held, e := f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
	require.True(t, held)
	require.ErrorContains(t, e, "needs the delete verb on secrets")
	require.NotContains(t, e.Error(), "intervention required")

	f.r.Client = api
	s, p, e = f.r.readOwnerPlan(t.Context(), f.tcp)
	require.NoError(t, e)
	require.NotNil(t, p)
	held, e = f.r.resumeOwnerPlan(t.Context(), f.tcp, f.cluster, s, p)
	require.NoError(t, e)
	require.False(t, held)
	_, p, e = f.r.readOwnerPlan(t.Context(), f.tcp)
	require.NoError(t, e)
	require.Nil(t, p)
	held, e = f.r.ownerUpdateHeld(t.Context(), f.tcp)
	require.NoError(t, e)
	require.False(t, held)
}

func TestConsumedPlanDeletionClassification(t *testing.T) {
	held, err := consumedPlanDeletion(apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, "plan"))
	require.False(t, held)
	require.NoError(t, err)
	held, err = consumedPlanDeletion(apierrors.NewConflict(schema.GroupResource{Resource: "secrets"}, "plan", nil))
	require.True(t, held)
	require.ErrorContains(t, err, "fresh read")
	held, err = consumedPlanDeletion(apierrors.NewServiceUnavailable("server text"))
	require.True(t, held)
	require.ErrorContains(t, err, "(ServiceUnavailable); retrying")
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package hooks tracks pending Cluster API runtime hooks on an object.
//
// This is a minimal port of sigs.k8s.io/cluster-api/internal/hooks, which is not importable
// from out-of-tree providers. The annotation format is part of the contract between the
// controller that owns a Machine and the core Machine controller, so it has to match
// upstream exactly.
//
// Derived from Cluster API, Copyright The Kubernetes Authors, Apache License 2.0.
package hooks

import (
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	runtimecatalog "sigs.k8s.io/cluster-api/api/runtime/catalog"
	runtimev1 "sigs.k8s.io/cluster-api/api/runtime/v1beta2"
)

// IsPending returns true if the given hook is tracked as pending on obj.
func IsPending(hook runtimecatalog.Hook, obj client.Object) bool {
	return isInList(obj.GetAnnotations()[runtimev1.PendingHooksAnnotation], runtimecatalog.HookName(hook))
}

func isInList(list, item string) bool {
	if list == "" {
		return false
	}

	return sets.New(strings.Split(list, ",")...).Has(item)
}

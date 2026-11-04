#!/bin/sh
# Fails unless go.mod vendors exactly the CABPT release named in CABPT_VERSION.
# CACPPT runs CABPT's pkg/updateoperation in-process (controllers/management.go:
# ObserveManagement), so a CABPT fix in that package only reaches a TalosControlPlane
# through this dependency; bumping the CABPT image alone changes nothing here.
set -eu
cd "$(dirname "$0")/.."
expected=$(tr -d '[:space:]' < CABPT_VERSION)
actual=$(sed -n 's#^replace github.com/siderolabs/cluster-api-bootstrap-provider-talos => github.com/Polychoronix/cluster-api-bootstrap-provider-talos \([^ ]*\)$#\1#p' go.mod)
if [ -z "$actual" ]; then
  echo "go.mod: CABPT replace directive not found" >&2
  exit 1
fi
if [ "$actual" != "$expected" ]; then
  echo "go.mod vendors CABPT $actual but CABPT_VERSION expects $expected; bump go.mod (require + replace) and go mod tidy before tagging" >&2
  exit 1
fi
echo "CABPT dependency $actual matches CABPT_VERSION"

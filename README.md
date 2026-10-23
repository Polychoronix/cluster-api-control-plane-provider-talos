# cluster-api-control-plane-provider-talos

<!-- textlint-disable -->
> [!CAUTION]
> Sidero Labs is no longer actively developing Cluster API Control Plane Provider Talos.
>
> For an alternative, please see [Omni](https://github.com/siderolabs/omni.git).
>
> Unless you have an existing support contract covering Cluster API Control Plane Provider Talos, all support will be provided by the community (including questions in our Slack workspace).
<!-- textlint-enable -->

## Intro

The Cluster API Control Plane Provider Talos (CACPPT) is a project by [Sidero Labs](https://www.SideroLabs.com/) that provides a [Cluster API](https://github.com/kubernetes-sigs/cluster-api)(CAPI) control plane provider for use in deploying Talos Linux-based Kubernetes nodes across any environment.
Given some basic info, this provider will generate control plane configurations for a given cluster and reconcile the necessary custom resources for CAPI to pick up the generated data.

## Corequisites

There are a few corequisites and assumptions that go into using this project:

- [Cluster API](https://github.com/kubernetes-sigs/cluster-api)
- [Cluster API Bootstrap Provider Talos](https://github.com/sidero-community/cluster-api-bootstrap-provider-talos)
- [Cluster API Provider Metal](https://github.com/siderolabs/cluster-api-provider-metal) (optional)

## Compatibility with Cluster API and Kubernetes Versions

This provider's versions are compatible with the following versions of Cluster API:

|                                              | v1alpha3 (v0.3) | v1alpha4 (v0.4) | v1beta1 (v1.x) | v1beta2 (v1.12+) |
| -------------------------------------------- | --------------- | --------------- | -------------- | ---------------- |
| Control Plane Provider Talos v1alpha3 (v0.2) | ✓               |                 |                |                  |
| Control Plane Provider Talos v1alpha3 (v0.3) |                 | ✓               |                |                  |
| Control Plane Provider Talos v1alpha3 (v0.4) |                 |                 | ✓              |                  |
| Control Plane Provider Talos v1alpha3 (v0.5) |                 |                 | ✓              |                  |
| Control Plane Provider Talos v1alpha3 (v0.6) |                 |                 |                | ✓                |
| Control Plane Provider Talos v1alpha3 (v0.7) |                 |                 |                | ✓                |
| Control Plane Provider Talos v1alpha3 (v0.8) |                 |                 |                | ✓                |
| Control Plane Provider Talos v1alpha3 (v0.9) |                 |                 |                | ✓                |

The `v0.6.x`, `v0.7.x`, `v0.8.x` and `v0.9.x` release series target the Cluster API `v1beta2` contract (CAPI core `v1.12+`; `v0.8.x` and later are built against `v1.14.2`, which adds Kubernetes 1.37 for management and workload clusters).
Released `v0.5.x` artifacts remain on `v1beta1`; `config/metadata/metadata.yaml` advertises the `v0.6` series as `v1beta2`.

The `v0.8.x` series drops `spec.controlPlaneConfig.init` (deprecated since v0.4.0) and the
`status.deprecated.v1beta1` block; v1alpha3 clients still see the old-shape status, derived from
the v1beta2 fields.

The `v0.9.x` series follows the Kubeadm Control Plane provider's machine lifecycle (see
[Machine deletion and etcd](#machine-deletion-and-etcd)) and removes the
`--enable-machine-pre-terminate-hook` flag: the hook is always on.

This provider's versions are able to install and manage the following versions of Kubernetes:

|                                              | v1.16 | v1.17 | v1.18 | v1.19 | v1.20 | v1.21 | v1.22 | v1.23 | v1.24 | v1.25 | v1.26 | v1.27 | v1.28 | v1.29 | v1.30 | v1.31 | v1.32 | v1.33 | v1.34 | v1.35 | v1.36 | v1.37 |
| -------------------------------------------  | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- |
| Control Plane Provider Talos v1alpha3 (v0.2) | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |
| Control Plane Provider Talos v1alpha3 (v0.3) | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |
| Control Plane Provider Talos v1alpha3 (v0.4) |       |       |       | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |       |       |       |       |       |       |       |       |       |       |       |
| Control Plane Provider Talos v1alpha3 (v0.5) |       |       |       |       |       |       |       |       |       |       | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |       |
| Control Plane Provider Talos v1alpha3 (v0.6) |       |        |       |       |       |       |       |       |       |       |       |       |       |       | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |       |
| Control Plane Provider Talos v1alpha3 (v0.7) |       |        |       |       |       |       |       |       |       |       |       |       |       |       | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |       |
| Control Plane Provider Talos v1alpha3 (v0.8) |       |        |       |       |       |       |       |       |       |       |       |       |       |       | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |
| Control Plane Provider Talos v1alpha3 (v0.9) |       |        |       |       |       |       |       |       |       |       |       |       |       |       | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |

This provider's versions are compatible with the following versions of Talos:

|                                              | v0.11 | v0.12  | v0.13 | v0.14 | v1.0  | v1.1  | v1.2  | v1.3  | v1.4  | v1.5  | v1.6  | v1.7  | v1.8  | v1.9  | v1.10 | v1.11 | v1.12 | v1.13 | v1.14 |
| -------------------------------------------- | ----- | ------ | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- |
| Control Plane Provider Talos v1alpha3 (v0.2) | ✓     | ✓      |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |
| Control Plane Provider Talos v1alpha3 (v0.3) | ✓     | ✓      | ✓     |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |       |
| Control Plane Provider Talos v1alpha3 (v0.4) | ✓     | ✓      | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |       |       |       |       |       |       |       |       |       |       |       |
| Control Plane Provider Talos v1alpha3 (v0.5) |       |        |       |       |       |       |       | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |       |
| Control Plane Provider Talos v1alpha3 (v0.6) |       |        |       |       |       |       |       |       |       |       |       |       |       |       | ✓     | ✓     | ✓     | ✓     | ✓     |       |
| Control Plane Provider Talos v1alpha3 (v0.7) |       |        |       |       |       |       |       |       |       |       |       |       |       |       | ✓     | ✓     | ✓     | ✓     | ✓     |       |
| Control Plane Provider Talos v1alpha3 (v0.8) |       |        |       |       |       |       |       |       |       |       |       |       |       |       | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |
| Control Plane Provider Talos v1alpha3 (v0.9) |       |        |       |       |       |       |       |       |       |       |       |       |       |       | ✓     | ✓     | ✓     | ✓     | ✓     | ✓     |

## Building and Installing

This control plane provider can be installed with clusterctl:

```bash
clusterctl init -c talos -b talos -i <infra-provider-of-choice>
```

If you encounter the following error, this is caused by a rename of our GitHub org from `talos-systems` to `siderolabs`.

```bash
$ clusterctl init -b talos -c talos -i sidero
Fetching providers
Error: failed to get provider components for the "talos" provider: target namespace can't be defaulted. Please specify a target namespace
```

This can be worked around by adding the following to `~/.cluster-api/clusterctl.yaml` and rerunning the init command:

```yaml
providers:
  - name: "talos"
    url: "https://github.com/sidero-community/cluster-api-bootstrap-provider-talos/releases/latest/bootstrap-components.yaml"
    type: "BootstrapProvider"
  - name: "talos"
    url: "https://github.com/sidero-community/cluster-api-control-plane-provider-talos/releases/latest/control-plane-components.yaml"
    type: "ControlPlaneProvider"
  - name: "sidero"
    url: "https://github.com/siderolabs/sidero/releases/latest/infrastructure-components.yaml"
    type: "InfrastructureProvider"
```

If you are going to use this provider as part of Sidero management plane, please refer to [Sidero Docs](https://www.sidero.dev/docs/v0.4/getting-started/install-clusterapi/)
on how to install and configure it.

This project can be built simply by running `make release` from the root directory.
Building requires Go 1.26+ (the `v0.8.x` and later series depend on CAPI `v1.14.2` / controller-runtime `v0.24`).
Doing so will create a file called `_out/control-plane-components.yaml`.
If you wish, you can tweak settings by editing the release yaml.
This file can then be installed into your management cluster with `kubectl apply -f _out/control-plane-components.yaml`.

Note that CACPPT should be deployed as part of a set of controllers for Cluster API.
You will need at least the upstream CAPI components, the Talos bootstrap provider, and an infrastructure provider for v1beta2 CAPI capabilities.

CACPPT plays the following role in the whole Cluster API architecture:

![Cluster API CACPPT](/docs/images/cacppt.png)

## Usage

### Supported Templates

You can use recommended [Cluster API templates](https://github.com/talos-systems/cluster-api-templates) provided by Sidero Labs.

It contains templates for `AWS` and `GCP`, which are verified by the integration tests.

### Creating Your Own Templates

If you wish to craft your own manifests, here is some important info.

CACPPT supports two API types:

- `TalosControlPlane` for direct control plane resources.
- `TalosControlPlaneTemplate` for ClusterClass / managed topology.

You can create YAML definitions of a `TalosControlPlane` and `kubectl apply` them as part of a larger CAPI cluster deployment.
Below is a bare-minimum example.

A basic config:

```yaml
apiVersion: controlplane.cluster.x-k8s.io/v1alpha3
kind: TalosControlPlane
metadata:
  name: talos-cp
spec:
  version: v1.31.0
  replicas: 1
  machineNamingStrategy:
    template: "{{ .talosControlPlane.name }}-{{ .random }}"
  machineTemplate:
    spec:
      infrastructureRef:
        apiGroup: infrastructure.cluster.x-k8s.io
        kind: DockerMachineTemplate
        name: talos-cp-machine-template
  controlPlaneConfig:
    controlplane:
      generateType: controlplane
      strategicPatches:
        - |
          machine:
            install:
              disk: /dev/sda
```

Direct `TalosControlPlane` resources must reference an infrastructure machine template via
`spec.machineTemplate.spec.infrastructureRef`.
See your infrastructure provider for how to craft the referenced machine template.

`spec.machineNamingStrategy` controls the names used for control plane `Machine` objects.
The corresponding infrastructure machine and Talos bootstrap config reuse the same name.
If you omit the field, the default template is `{{ .talosControlPlane.name }}-{{ .random }}`.
Custom templates must include `{{ .random }}` and can use:

- `.cluster.name`
- `.talosControlPlane.name`
- `.random`

`strategicPatches` is an array of strings.
Each patch must be passed as a YAML string, typically via a block scalar (`- |`), not as an inline object.

Note the generateType mentioned above.
This is a required value in the spec for both controlplane and worker ("join") nodes.
For a no-frills control plane config, you can simply specify `controlplane` depending on each config section.
Generated Talos client credentials are held only in the namespace-local
`<cluster>-talosconfig` Secret, key `talosconfig`, not public `TalosConfig` status.
Use an authorized private credential-handling workflow; never paste this Secret,
full machine configuration or credential bytes into logs, chat or Git.

If you wish to do something more complex, we allow for the ability to supply an entire Talos machine config file to the resource.
This can be done by setting `controlPlaneConfig.controlplane.generateType` to `none` and specifying a `data` field.
This config file can be generated with `talosctl gen config` and then edited to supply the various options you may desire.
When you provide `data` this way, the bootstrap provider uses the supplied Talos machine configuration as-is instead of generating one for you.

`controlPlaneConfig.controlplane` accepts every field of the bootstrap provider's `TalosConfigSpec`, including `imageFactory`: the Talos Image Factory schematic (extensions, extra kernel arguments, overlay, bootloader) the control plane machines install and upgrade with. The bootstrap provider registers the schematic and renders the installer image from it (an `UnattendedInstallConfig` document from Talos 1.14, `machine.install.image` before); see its README for the field reference. Changing the block rolls the control plane like any other `controlPlaneConfig` change.

An example of a more complex config:

```yaml
apiVersion: controlplane.cluster.x-k8s.io/v1alpha3
kind: TalosControlPlane
metadata:
  name: talos-0
  labels:
    cluster.x-k8s.io/cluster-name: talos
spec:
  controlPlaneConfig:
    controlplane:
      generateType: none
      data: |
        version: v1alpha1
        machine:
          type: controlplane
        cluster:
          token: xxxxxx
        ...
```

When you manage the full machine configuration yourself, you should also keep track of the Talos client configuration you generated alongside it.


### ClusterClass / managed topology

For managed topology, define a `TalosControlPlaneTemplate` and reference it from a `ClusterClass`.
These examples use `cluster.x-k8s.io/v1beta2`, which matches the contract targeted by the `v0.6.x` release series.
The template example below intentionally omits `machineTemplate.spec.infrastructureRef`; for ClusterClass-managed topology, that reference comes from `ClusterClass.spec.controlPlane.machineInfrastructure.templateRef`.
`TalosControlPlaneTemplate.spec.template.spec` is immutable after creation, so create a new template resource when you need to roll out a spec change.

```yaml
apiVersion: controlplane.cluster.x-k8s.io/v1alpha3
kind: TalosControlPlaneTemplate
metadata:
  name: talos-cp-template
spec:
  template:
    spec:
      machineNamingStrategy:
        template: "{{ .talosControlPlane.name }}-{{ .random }}"
      machineTemplate:
        metadata:
          labels:
            example.siderolabs.dev/control-plane: "true"
      controlPlaneConfig:
        controlplane:
          generateType: controlplane
          strategicPatches:
            - |
              machine:
                install:
                  disk: /dev/sda
---
apiVersion: cluster.x-k8s.io/v1beta2
kind: ClusterClass
metadata:
  name: talos-quickstart
spec:
  controlPlane:
    naming:
      template: "{{ .cluster.name }}-control-plane"
    templateRef:
      apiVersion: controlplane.cluster.x-k8s.io/v1alpha3
      kind: TalosControlPlaneTemplate
      name: talos-cp-template
    machineInfrastructure:
      templateRef:
        apiVersion: infrastructure.cluster.x-k8s.io/v1beta2
        kind: DockerMachineTemplate
        name: talos-cp-machine-template
---
apiVersion: cluster.x-k8s.io/v1beta2
kind: Cluster
metadata:
  name: talos-topology
spec:
  topology:
    classRef:
      name: talos-quickstart
    version: v1.31.0
    controlPlane:
      replicas: 3
```

For ClusterClass / topology, the infrastructure machine template is supplied via
`ClusterClass.spec.controlPlane.machineInfrastructure.templateRef`, and the topology controller
populates `TalosControlPlane.spec.machineTemplate.spec.infrastructureRef` on the generated
concrete control plane object.

For naming in managed topology there are two levels:

- `ClusterClass.spec.controlPlane.naming.template` controls the generated `TalosControlPlane` name.
- `TalosControlPlaneTemplate.spec.template.spec.machineNamingStrategy` controls the generated control plane `Machine`, infrastructure machine, and Talos bootstrap config names.

With the example above, a cluster named `talos-topology` produces a concrete `TalosControlPlane`
named `talos-topology-control-plane`, and control plane machines named
`talos-topology-control-plane-xxxxx`.

See `config/samples/topology_v1alpha3_clusterclass_with_taloscontrolplanetemplate.yaml` for a ClusterClass / Cluster fragment including workers.
That sample assumes the referenced infrastructure and worker bootstrap templates already exist.

### Secret-backed inputs and per-Machine Talos endpoints

The matched bootstrap provider's `configSecretRef`, ordered
`strategicPatchSecretRefs`, `configRevision`, `machineConfig` and `talosEndpoint`
are preserved in `spec.controlPlaneConfig.controlplane` and templates, including
served-version conversion. References are `{name, key}` in the TalosConfig's
namespace. A baseline replaces only the base; shared public and Secret patches
remain ordered policy. A changed public spec triggers a requested update; use a
new non-secret `configRevision` for changed bytes in a same-name Secret. There is
no automatic Secret drift rollout.

Use `spec.machineConfigOverrides` for existing per-node differences. The list is
keyed by unique Machine `name`, with required exact `uid` and at least one
`config` (the bootstrap provider's `MachineConfigOverlay`) or `talosEndpoint`:

```yaml
machineConfigOverrides:
  - name: example-control-plane-one
    uid: <actual-existing-Machine-UID>
    config:
      configSecretRef: {name: example-node-baseline, key: value}
      configRevision: reviewed-node-revision
    talosEndpoint:
      address: talos-egress.example:50000
      serverName: example-control-plane-one.example.ts.net
      node: example-control-plane-one
```

Namespace is implicit; no cross-namespace input. A reused name with another UID
is a hard error **before** pre-terminate, etcd or lifecycle actions. Missing entries
may remain inert before association. New Machines use shared-only inputs, and a
new name colliding with a pinned entry fails rather than consuming it. An override
`config` cannot coexist with shared `controlPlaneConfig.controlplane.machineConfig`:
there is only one per-node slot. Put shared policy in the root public/Secret patch
fields. Endpoint-only overrides may coexist with the shared slot. Every other
shared field stays intact in creation, rollout equality and update plans.

Each Machine gets its own imported-mTLS client: `address` is dial transport,
`serverName` verifies TLS/SNI, and optional `node` routes every RPC through a proxy.
Different identities are never combined in an endpoint list. Kubernetes node IPs
are not changed; ordinary provisioning retains node-address fallback when no
explicit endpoint exists. These input and transport seams **do not authorize
adoption** or suppress the provider's normal etcd/lifecycle actions. Keep the
existing observer until a separately approved adoption guard and complete inventory
are available.

Local tests exercise effective specs, UID/conflict fencing, actual served CRD
admission and per-node mTLS RPCs (health, etcd, bootstrap and leave/remove), including
distinct dial/SNI/targets and unchanged imported credentials. This is not a live
adoption or verified Talos update-completion claim.

### Observed control-plane update status

Machine `UpToDate` is `False` with reason `Updating` while a spec-matched Machine
has pending/in-progress update markers, core's `Updating=True`, or an observed
kubelet version different from its desired version. This follows core CAPI and
KCP's condition contract; spec matching alone is not recovery completion.

Control-plane `status.version` comes only from **every associated non-deleting
Machine's running and ready `kube-apiserver` container status**, via uncached
Node/Pod reads through the existing ClusterCache kubeconfig transport. Talos
v1.14 static Pods use namespace `kube-system`, label `component=kube-apiserver`
and container name `kube-apiserver`. Machine NodeRef associations must be unique
and present; providerIDs must match where available. ContainerStatus image tags
(including version-tag@digest) are observed, not Pod.spec desired images or one
load-balanced `/version`. Missing/duplicate Pods, denied reads, unready containers
and opaque/digest-only tags cannot establish a version.

`VersionObserved` is false on incomplete inspection or outstanding update
completion. A new running image cannot advance the published version while any
Machine update remains pending. On the first observation pass, an inherited
status.version lacking this new condition is discarded as unverified legacy
intent. Complete successful observations establish the minimum and provenance;
later temporary failures retain only that previously confirmed value and set the
condition false. This condition is not adoption consent or a durable operation
receipt. Ordinary fresh-bootstrap UpToDate spec semantics remain unchanged.

Workload credentials need read-only Nodes and `kube-system` Pods access; the
intended external auth-proxy role already grants these, but deployed permissions
must be verified by the user. No new credentials, management privileges or
transport migration are introduced. Isolated owner envtest covers status/replica
holds and advancement; real token-only TLS smoke proves uncached per-node reads
and denial without modifying the externally managed Secret.

### Externally managed Kubernetes kubeconfig

Set `spec.kubeconfig.mode: External` on a `TalosControlPlane` (or
`spec.template.spec.kubeconfig.mode` on its template) to retain an independently
managed Kubernetes auth-proxy kubeconfig. The omitted/default mode is `Managed`.
Both served control plane API versions preserve the setting.

External mode consumes only the standard namespace-local `<cluster>-kubeconfig`
Secret, key `value`; it never creates a replacement, takes ownership, rewrites
labels/credentials, or attempts client-certificate rotation. The Secret's external
owner must provide the matching `cluster.x-k8s.io/cluster-name` label required by
the CAPI Cluster cache. Token authentication works without a Kubernetes client
certificate; exec and auth-provider plugins are unsupported by core CAPI and
are rejected without execution. Supply valid HTTPS server/current-context
configuration. The kubeconfig's `tls-server-name` may differ from its dial host.
Missing or invalid input reports `KubeconfigAvailable=False` using safe messages;
Kubernetes API reachability is separately observed through `Available` and never
inferred from successful kubeconfig parsing. Inspect metadata/conditions only:

```sh
kubectl get secret <cluster>-kubeconfig -n <namespace> \
  -o custom-columns=NAME:.metadata.name,UID:.metadata.uid,OWNERS:.metadata.ownerReferences[*].name
kubectl get taloscontrolplane <name> -n <namespace> \
  -o jsonpath='{.status.conditions}'
```

The local `TestExternalKubeconfigClusterCacheTransport` smoke exercises the real
CAPI ClusterCache and TLS REST client with token-only auth, separate dial host/SNI,
and no client certificate. It observes repeated Kubernetes API requests, unchanged
Secret bytes/metadata, stable healthy condition transition timestamps, and actual
denial/recovery transitions. It uses disposable envtest management storage and an
in-memory TLS workload endpoint, never an ambient/live cluster:

```sh
KUBEBUILDER_ASSETS=<setup-envtest-binary-directory> go test ./controllers \
  -run '^TestExternalKubeconfig' -count=1 -v
```

For kissanpentu, retain `kissanpentu-kubeconfig` and its validated auth-proxy path.
This is not an adoption guard: normal Machine/etcd lifecycle operations remain
active. Keep the existing observer and do not transfer `controlPlaneRef` until an
explicit guarded inventory/handoff is approved. Native Kubernetes TCP access or
returning to Managed mode is a deliberate credentials/ownership migration, not a
side effect of updating Talos management endpoints.

### In-place update serialization

Enable `RuntimeSDK=true,InPlaceUpdates=true` on core Cluster API and
`InPlaceUpdates=true` on this control plane provider, with the matched Talos
bootstrap runtime extension registered. The provider starts one control plane
update at a time. Writing the desired Machine and bootstrap specs is **not**
completion: while any control plane Machine carries the update-in-progress or
pending `UpdateMachine` annotation, no other update, replacement, scale-up or
scale-down starts. The hold also applies when every spec already matches, after
an owner restart, or if the feature gate is disabled during an existing update.
Do not remove these annotations to force advancement. A namespace-local private
TCP-owned Secret freezes the exact owner plan before any desired-state write.
Prepared plans resume only UID/resourceVersion-guarded API writes; missing objects
are never recreated. Referenced Secret UID/RV/content snapshots must remain
unchanged through rendering. Current-generation output freshness is frozen before
Released, then the pending hook is exposed. Restarted Released plans without an
exact completion witness stop for intervention, never restamp or re-execute.

Private active/ambiguous plans fence pre-terminate/etcd audit, deletion, scale and
deferred UpToDate/version advancement even when public flags disappear. Actual
read-only health observations may continue. Completion requires the exact CABPT
Observed receipt, accepted image intent, original object identities and all three
core update flags plus pending hook removed. Receipt consumption, owner Consumed
CAS, matching TalosConfig plan-reference detach and owner-plan UID/RV deletion
are individually restart-safe. CABPT's journal/receipt remains intact for the next
transaction; it is never deleted by this provider.

Talos reboot draining is separate from CAPI's Machine deletion drain and volume
detach pipeline described below. In-place updates keep the Machine and
InfraMachine. Owner recovery proves safe API transaction completion, not physical
Talos reboot/image/storage recovery; the actual CABPT executor must establish
those observations before emitting its receipt.
Physical wipe, including a Robot's original configuration seed, remains manual.

The retained controller regressions use disposable envtest API servers/etcd and
real UID/RV no-create writes. They cover concurrent owners, response loss, stale
resource versions, source/object replacement, receipt cleanup and a pending hook
with two control plane candidates across gate-on/off and replica counts. Output
and completion receipts in these isolated owner tests are explicit fixtures, not
actual CABPT/core recovery proof. No ambient cluster is used:

```sh
KUBEBUILDER_ASSETS=<setup-envtest-binary-directory> go test ./controllers \
  -run '^(TestOwner|TestReconcileInPlaceUpdates|TestPreflight_HoldsUpdatingMachineEvenWhenExcluded|TestInPlaceSerializationControllerEnvtest)' -count=1 -v
```

The separate opt-in `TestOwnerActualCABPTCoreReceiptCoupling` exercises the
**actual** CACPPT owner/runtime caller and ClusterCache, a standalone CABPT
manager, and the core CAPI executor against disposable APIs. It first creates
fresh bootstrap configurations through the provider, waits for CABPT-generated
accepted image intent before simulated provisioning/Node readiness, then applies
a shared revision. It verifies no receipt or peer Apply before recovery, followed
by the original transaction's Observed/consumed receipt, accepted intent, core
cleanup, detached plan references and retired owner Secret UID. A new transaction
for the peer may subsequently occupy the deterministic owner-plan name.

The exercised standalone CABPT source was local commit
`f19b8fa1629b5a8bcc786ac0de3b0100f88bb809`; the core source/API was CAPI v1.14.2
commit `5f7d58c9ad647f31bd342bf8de33092c27f7fc38`. These are source observations,
not published release/image pins. Prepare the actual core binary and its source
tree, Kubernetes 1.36.2 envtest binaries and the CAPH v1.1.8 CRD directory before
running; the test does not download assets or use ambient credentials. Build the
standalone CABPT manager from the matched checkout, one compile at a time:

```sh
# Run in the matched CABPT checkout.
GOMAXPROCS=2 CGO_ENABLED=0 GOWORK=off go build -p 1 -o /tmp/cacppt-cabpt-manager .

# Run in this repository. GOWORK is only for unpublished matched local sources;
# keep that workspace outside the repository and never ship a local-path replace.
GOMAXPROCS=2 CGO_ENABLED=0 \
GOWORK=<external-matched-workspace-file> \
KUBEBUILDER_ASSETS=<k8s-1.36.2-envtest-binary-directory> \
CAPI_RUNTIME_CORE_BINARY=<absolute-core-v1.14.2-manager-binary> \
CAPI_RUNTIME_CORE_SOURCE=<absolute-core-v1.14.2-source-root> \
CAPI_RUNTIME_CAPH_CRDS=<absolute-caph-v1.1.8-crd-directory> \
CABPT_RUNTIME_BINARY=/tmp/cacppt-cabpt-manager \
go test -p 1 ./controllers -run '^TestOwnerActualCABPTCoreReceiptCoupling$' \
  -count=1 -v -timeout=6m
```

The core source root must contain `core/config/crd/bases` and
`core/config/webhook/manifests.yaml`; the CAPH directory must contain its actual
HetznerCluster, HetznerBareMetalMachine and template CRDs. The test launches both
managers with explicit generated local kubeconfigs and separate TLS certificates,
leaves `POD_NAMESPACE` unset for CABPT's URL registration fixture, and waits for
core's real Discovery. Runtime poll cadence is unchanged; the bounded completion
window accounts for the advertised 30-second retries. Children are stopped before
API/certificate cleanup. No manual accepted-intent, journal or receipt writes are
used in this test.

This is a **two-CP, no-reboot cross-provider proof with simulated mTLS/COSI node
backends**, not a three-node physical rollout, adoption or OS-upgrade proof.
Fixture-only restart/serialization tests above remain separate. Explicit captured
owner/manager/context and subprocess logs are checked for private-token redaction;
framework-global certwatcher logs are not claimed as covered by that capture.


Fresh TalosConfig creation records creation-only initial TCP name/UID annotations
for CABPT's live initial-bootstrap lineage validation. Updates preserve them;
existing configs/adopted nodes never receive them retrospectively. These markers
are not adoption consent.

### Runtime extension namespace scope and settings

The control plane's narrow runtime caller honors the registered
`ExtensionConfig.spec.namespaceSelector` against the **Machine namespace's**
labels, not Machine labels or the extension Service namespace. Core CAPI
admission defaults an omitted selector to `{}`, which matches every namespace.
Use `matchLabels` or `matchExpressions` to restrict the extension's scope. The
provider needs only `get` permission on Namespaces for this lookup.

Discovery and the actual call independently read the current registration and
namespace labels. A previously discovered handler cannot bypass a subsequently
narrowed selector or relabeled namespace. Invalid/missing selectors and API read
errors are hard failures, not empty discovery or permission to replace a Machine.
A valid selector excluding a namespace makes that extension unavailable there.

`spec.settings` is included in hook requests, with request-specific values taking
precedence. Neither the caller's request nor the registration is modified; the
request is copied only when registration settings must be added. This is not a
general-purpose replacement for the internal core CAPI runtime client. In-place
handlers must use `failurePolicy: Fail` (also core's default). `Ignore` is
unsupported and rejected: call errors must not become successful responses or
silently authorize replacement of an ambiguous operation.

The local regression uses real CAPI Runtime SDK TLS Discovery and
`CanUpdateMachine`, with Discovery-derived handler status persisted in a disposable
envtest management API server. It checks request settings and returned coverage,
selected/excluded namespaces, relabeling, selector narrowing, zero excluded calls,
and successful recovery. It does **not** run the CABPT executor or core
Machine/ExtensionConfig reconcilers, prove Talos completion, or access a live
cluster:

```sh
KUBEBUILDER_ASSETS=<setup-envtest-binary-directory> go test ./internal/runtimeclient -count=1 -v
```

### Machine deletion and etcd

Control plane `Machine` deletion follows the Kubeadm Control Plane provider's pattern, on every
deletion path — a scale-down, a rollout, a `MachineHealthCheck` remediation, or a plain
`kubectl delete machine`:

1. **Interception.** Every control plane `Machine` this provider owns carries the Cluster API
   pre-terminate lifecycle hook `pre-terminate.delete.hook.machine.cluster.x-k8s.io/tcp-cleanup`
   from the moment it is created. Machines that predate the hook are adopted on the next
   reconcile — with one exception: a `Machine` that is *already* being deleted is never stamped,
   because it may have moved past the pre-terminate phase already. While the annotation is
   present, the core Machine controller holds the `Machine` after the node has been drained and
   its volumes detached, and before the infrastructure provider is allowed to delete the
   `InfraMachine`.
2. **etcd member removal.** At that point the provider verifies through a healthy peer that the
   machine is still a member, checks that removing it leaves etcd with a healthy majority (see
   the quorum safeguard below), asks the machine itself to forfeit leadership and leave, and
   falls back to removing the member through the peer when the machine is already gone.
3. **Infrastructure teardown.** Releasing the hook is all the provider does to let the deletion
   continue. It never removes a member ahead of the deletion request and never deletes workload
   `Node` objects: the core Machine controller deletes the `Node` once the infrastructure is gone.
   A scale-down therefore consists of picking the machine and deleting it, exactly like a manual
   deletion.
4. **Self-healing.** Deleting machines still count as replicas, so nothing is created while a
   deletion is in flight. Once the `Machine` is gone the provider creates a replacement — after
   the preflight checks below pass.

The guarantee is that **etcd membership is resolved before any infrastructure provider powers off
or reprovisions the node, on every deletion path**, and that no deletion or replacement ever
starts while the control plane cannot take it.

Whole-cluster and whole-control-plane teardown skip the etcd work entirely and release the hook
immediately: during a full teardown there is no quorum left to hand membership to.

#### Preflight checks

Like KCP, the provider runs preflight checks before creating or deleting a control plane
`Machine`. A scale operation waits while:

- any control plane `Machine` is being deleted (one membership change at a time),
- any control plane `Machine` has an in-place transaction pending or in progress,
- any `Machine` has no `Node` yet,
- a control plane component reports an unhealthy service, or
- the `EtcdClusterHealthy` condition is not `True`.

The machine a scale-down is about to delete is exempt from the per-machine checks, so a rollout
can still replace a broken machine. While the checks fail, the `Resized` condition carries the
reason (for example `Scaling up control plane to 3 replicas (actual 2): waiting for machines to
be deleted: cp-2`), a `ControlPlaneUnhealthy` warning event is recorded on the
`TalosControlPlane`, and the operation is retried after 10 seconds.

#### Quorum safeguard

Before a member leaves, the provider counts the remaining machines whose etcd reports healthy —
every owned machine but the one being deleted, including machines that are themselves deleting
while their etcd still runs — against the majority the cluster needs once the member is gone
(`(members - 1) / 2 + 1`). An unreachable machine counts as unhealthy. If that majority would not
survive, the deletion is **held**: the hook stays, an `EtcdQuorumAtRisk` warning event is recorded
on the `Machine`, and the check is retried until the other members recover. Unlike the failures
below, this hold never times out; losing quorum is worse than a parked deletion.

Flags:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--etcd-cleanup-timeout` | `2m` | How long etcd member removal is retried before the provider gives up. |

The timeout fails open. If neither the graceful leave nor the removal through a peer succeeds
before it expires, the provider emits an `EtcdCleanupOrphaned` warning event on the `Machine` and
releases the hook anyway, so a deletion is never parked forever; the leftover member is collected
by the periodic etcd audit once the cluster is stable again. The clock only measures time spent
actually trying to remove the member: a quorum hold resets it. The other outcomes are reported
as `EtcdMemberLeft`, `EtcdMemberRemovedViaPeer` and `EtcdCleanupSkipped` events.

The escape hatch, if you need a `Machine` to finish deleting right now — including out of a
quorum hold:

```bash
kubectl annotate machine <name> pre-terminate.delete.hook.machine.cluster.x-k8s.io/tcp-cleanup-
```

#### Coexistence with reset-style hooks

Reset-style pre-terminate hooks (for example a `talos-machine-teardown` hook that wipes the disks
and halts the node) are designed to run **last**, once nothing else needs the machine. This
provider's hook deliberately runs **first**: it does not wait for other pre-terminate hooks to
finish, unlike KCP, which defers to user hooks so that kubelet keeps working while they run.

The reason is that a reset hook takes the node down for good, so the etcd member has to be gone
before it runs — and if both hooks insisted on running last, the deletion would deadlock. So the
ordering is: Cluster API drains the node, this provider removes the etcd member and releases its
hook, and the reset hook then runs on a node that is no longer part of the etcd cluster.


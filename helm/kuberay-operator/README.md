# kuberay-operator

Helm wrapper chart for the [KubeRay operator](https://github.com/ray-project/kuberay) (`kuberay-operator 1.7.0`) deployed in the `kuberay-operator` namespace.

## What it does

- Installs the KubeRay operator, which watches and reconciles Ray custom resources on the cluster:
  `RayCluster`, `RayJob`, `RayService`, `RayCronJob`.
- Installs the associated CRDs.
- This is cluster-wide infrastructure, not an application — it must be installed before any `RayService`/`RayCluster` workload (e.g. a future `mlmodel-ray`) can be deployed.

## Key values

| Value | Default |
|---|---|
| `kuberay-operator.image.tag` | `v1.7.0` |
| `kuberay-operator.replicas` | `1` |

## Adding a Ray workload

Once this operator is running, define a `RayService`/`RayCluster` manifest in its own `helm/<name>` + `apps/<name>/application.yaml` (same pattern as every other component here), targeting a sync-wave after this one (`"0"`) so the CRDs/operator exist first.

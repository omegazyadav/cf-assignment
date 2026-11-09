# cf-assignment

Scalable multi-model inference service. [docs/DESIGN.md](docs/DESIGN.md) is the proposed design: models hosted on Ray (Ray Serve on KubeRay), with diagrams. This repo also contains a runnable **working example** of a subset of it: Ray Serve model versions deployed through ArgoCD. The example model image is built from [`mlmodel/`](mlmodel) by `.github/workflows/mlmodel-image.yml`.

Working-example diagrams (Mermaid, rendered by GitHub): [architecture](docs/diagrams/architecture.md), [adding a version](docs/diagrams/rollout.md).

## Prerequisites

- Kubernetes cluster with ArgoCD installed in the `argocd` namespace
- `kubectl` pointed at the cluster

## Deploy

```bash
make install
make status
```

`make install` runs:

1. `install-ray` - KubeRay operator
2. `install-models` - applies `apps/models/applicationset.yaml`, which generates one ArgoCD Application and one RayService per model version (random-forest 1-0-0, 2-0-0; logistic-regression 1-0-0, 2-0-0)

## Try it

```bash
make predict FAMILY=random-forest VERSION=1-0-0
```

Each version has its own service and route prefix, e.g. `/predict`. In the ArgoCD UI every version is a separate Application, labelled with `model` and `version`.

## Add a model version

Add `- family: <family>` / `version: <x-y-z>` to the list in `apps/models/applicationset.yaml` and push; ArgoCD generates the new Application and RayService. The artifact `mlmodel/model/<family_with_underscores>-<version>.joblib` must exist in the image (add it to `mlmodel/src/train.py`, then set `image.tag` in `helm/mlmodel-ray/values.yaml` to the new build). Remove a version by deleting its list entry.

## Remove

```bash
make uninstall
```

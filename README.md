# cf-assignment

Scalable multi-model inference service: design in [docs/DESIGN.md](docs/DESIGN.md), plus a runnable demo that deploys Ray Serve model versions through ArgoCD.

## Prerequisites

- Kubernetes cluster with ArgoCD installed in the `argocd` namespace
- `kubectl` pointed at the cluster, `gh` authenticated (used to give ArgoCD read access to this private repo)

## Deploy

```bash
make install
make status
```

`make install` runs:

1. `repo-credentials` - registers this private repo in ArgoCD using `gh auth token` (override with `GITHUB_TOKEN=...`)
2. `install-ray` - KubeRay operator
3. `install-models` - two RayServices: `random-forest` (versions 1-0-0, 1-0-1, 2-0-0) and `logistic-regression` (1-0-0)

## Try it

```bash
make port-forward FAMILY=random-forest VERSION=1-0-0
make predict FAMILY=random-forest VERSION=1-0-0
```

Every version of a family is served by the same port-forwarded service under its own route prefix, e.g. `/random-forest-2-0-0/predict`.

## Add a model version

Add an entry to `serve.applications` in the family's `apps/<family>/application.yaml` (name, `modelPath`, `routePrefix`, autoscaling bounds) and push; ArgoCD syncs it.

## Remove

```bash
make uninstall
```

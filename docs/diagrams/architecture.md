# Demo architecture

```mermaid
flowchart LR
    dev([Developer]) -->|git push| repo[(GitHub repo<br/>cf-assignment, private)]
    dev -->|make install| mk[Makefile]
    mk -->|repo-credentials secret| argo
    mk -->|kubectl apply apps/*| argo

    subgraph cluster[Kubernetes cluster]
        argo[ArgoCD]
        argo -->|sync| op

        subgraph nsop[ns: kuberay-operator]
            op[KubeRay operator]
        end

        subgraph nsml[ns: mlops]
            subgraph rf[RayService random-forest]
                rfhead[Ray head]
                rfw[Ray workers<br/>autoscaled]
                rfsvc[Service serve-svc :8000]
                rfsvc --> v100[Serve app random-forest-1-0-0<br/>1-4 replicas]
                rfsvc --> v101[Serve app random-forest-1-0-1<br/>1-4 replicas]
                rfsvc --> v200[Serve app random-forest-2-0-0<br/>1-4 replicas]
            end
            subgraph lr[RayService logistic-regression]
                lrhead[Ray head]
                lrw[Ray workers<br/>autoscaled]
                lrsvc[Service serve-svc :8000]
                lrsvc --> lr100[Serve app logistic-regression-1-0-0<br/>1-2 replicas]
            end
        end

        op -->|manages| rf
        op -->|manages| lr
    end

    repo -->|pulled by| argo
    hub[(Docker Hub<br/>omegazyadav/mlmodel)] -->|image pull| rfhead
    hub --> lrhead
    user([Client]) -->|kubectl port-forward| rfsvc
    user --> lrsvc
```

- Each model family is its own RayService (own Ray cluster), so one family's failure or redeploy does not affect the other.
- Each version is a Serve application with its own route prefix (for example `/random-forest-2-0-0/predict`) and its own replica bounds, so versions scale independently.
- Replicas scale on `target_ongoing_requests`; the Ray autoscaler (`enableInTreeAutoscaling`) adds or removes worker pods when replicas need more capacity.

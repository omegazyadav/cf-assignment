"""Ray Serve entrypoint for the model API, parameterized by model artifact.

`entrypoint` is a builder function: each call binds a
fresh MLModelDeployment pointed at whichever model_path it's given. This lets
the RayService chart (helm/mlmodel-ray) deploy multiple
applications from the same import_path - each a different model, or a
different version of the same model - every one with its own route_prefix
and its own autoscaling_config, scaling independently of the others.
"""
from __future__ import annotations

from pathlib import Path
from typing import List

import joblib
from fastapi import FastAPI
from pydantic import BaseModel, Field
from ray import serve

api = FastAPI(title="MLOps Model API", version="0.1.0")


class PredictRequest(BaseModel):
    features: List[float] = Field(
        ...,
        description="Iris features: [sepal_length, sepal_width, petal_length, petal_width]",
        min_length=4,
        max_length=4,
    )


class PredictResponse(BaseModel):
    prediction: int
    label: str
    probabilities: dict


@serve.deployment(ray_actor_options={"num_cpus": 0.5})
@serve.ingress(api)
class MLModelDeployment:
    def __init__(self, model_path: str):
        self.model_path = Path(model_path)
        if not self.model_path.exists():
            raise RuntimeError(f"Model file not found at {self.model_path}")
        self.bundle = joblib.load(self.model_path)

    @api.get("/health")
    def health(self) -> dict:
        return {"status": "ok", "model_loaded": True, "model_path": str(self.model_path)}

    @api.get("/")
    def root(self) -> dict:
        return {"model_path": str(self.model_path), "docs": "/docs", "health": "/health"}

    @api.post("/predict", response_model=PredictResponse)
    def predict(self, req: PredictRequest) -> PredictResponse:
        model = self.bundle["model"]
        target_names = self.bundle["target_names"]

        pred = int(model.predict([req.features])[0])
        proba = model.predict_proba([req.features])[0]
        return PredictResponse(
            prediction=pred,
            label=target_names[pred],
            probabilities={name: float(p) for name, p in zip(target_names, proba)},
        )


def entrypoint(args: dict):
    """Serve's builder-function hook. Ray Serve calls builder functions with a
    single dict positional argument (the whole `args:` mapping from
    serveConfigV2), not unpacked kwargs - so `model_path` must be pulled out
    of `args` here rather than declared as a named parameter."""
    model_path = args.get("model_path", "/app/model/random_forest-1-0-0.joblib")
    return MLModelDeployment.bind(model_path=model_path)

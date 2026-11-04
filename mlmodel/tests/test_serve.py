from pathlib import Path

import pytest
import httpx
from ray import serve

from app.ray_serve import entrypoint

MODEL = Path(__file__).resolve().parents[1] / "model" / "random_forest-1-0-0.joblib"


@pytest.fixture(scope="module")
def base_url():
    serve.run(entrypoint({"model_path": str(MODEL)}), route_prefix="/rf")
    yield "http://127.0.0.1:8000/rf"
    serve.shutdown()


def test_health(base_url):
    r = httpx.get(f"{base_url}/health")
    assert r.status_code == 200
    assert r.json()["model_loaded"] is True


def test_predict(base_url):
    r = httpx.post(f"{base_url}/predict", json={"features": [5.1, 3.5, 1.4, 0.2]})
    assert r.status_code == 200
    assert r.json()["label"] == "setosa"


def test_predict_rejects_bad_payload(base_url):
    r = httpx.post(f"{base_url}/predict", json={"features": [1.0]})
    assert r.status_code == 422

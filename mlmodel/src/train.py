"""Train Iris classifiers and save them to model/<name>.joblib.

Trains several model/version combinations in one run so they can all be
served simultaneously (see helm/mlmodel-ray multi-application
RayService config) - e.g. comparing algorithms, or comparing versions of the
same algorithm. `random_forest-1-0-0.joblib` (n_estimators=100) is kept as
the default/stable artifact used by the Ray Serve entrypoint and tests.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import joblib
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

MODEL_DIR = Path(__file__).resolve().parent.parent / "model"
METRICS_PATH = MODEL_DIR / "metrics.json"

# name (used as model/<name>.joblib) -> (estimator factory, params)
MODELS: Dict[str, Dict[str, Any]] = {
    "random_forest-1-0-0": {
        "estimator": RandomForestClassifier,
        "params": {"n_estimators": 100, "random_state": 42},
    },
    "random_forest-2-0-0": {
        "estimator": RandomForestClassifier,
        "params": {"n_estimators": 300, "random_state": 42},
    },
    "logistic_regression-1-0-0": {
        "estimator": LogisticRegression,
        "params": {"max_iter": 400, "random_state": 42},
    },
    "logistic_regression-2-0-0": {
        "estimator": LogisticRegression,
        "params": {"max_iter": 600, "C": 0.5, "random_state": 42},
    },
}

def main() -> None:
    data = load_iris()
    X_train, X_test, y_train, y_test = train_test_split(
        data.data, data.target, test_size=0.2, random_state=42, stratify=data.target
    )

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    metrics: Dict[str, float] = {}

    for name, spec in MODELS.items():
        clf = spec["estimator"](**spec["params"])
        clf.fit(X_train, y_train)

        acc = accuracy_score(y_test, clf.predict(X_test))
        metrics[name] = acc
        print(f"[{name}] test accuracy: {acc:.4f}")

        out_path = MODEL_DIR / f"{name}.joblib"
        joblib.dump(
            {
                "model": clf,
                "target_names": list(data.target_names),
                "feature_names": list(data.feature_names),
            },
            out_path,
        )
        print(f"Saved model to {out_path}")

    METRICS_PATH.write_text(json.dumps(metrics, indent=2))

if __name__ == "__main__":
    main()

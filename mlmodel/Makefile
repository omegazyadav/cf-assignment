.PHONY: install train test lint docker-build clean

install:
	pip install -r requirements-dev.txt

train:
	python src/train.py

test: train
	pytest -q

lint:
	ruff check app src tests

docker-build: train
	docker build -t mlmodel:local .

clean:
	rm -rf model/*.joblib model/*.json __pycache__ .pytest_cache

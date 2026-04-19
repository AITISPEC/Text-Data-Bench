.PHONY: install lint test run clean

install:
	pip install -e ".[dev]"

lint:
	ruff check src tests
	ruff format --check src tests

test:
	pytest -v -m "not slow"

run:
	databench process tests/fixtures/sample.txt output/clean.parquet --config configs/pipeline.yaml

clean:
	rm -rf output/ __pycache__/ .pytest_cache/ build/ dist/ *.egg-info
	find . -type d -name __pycache__ -exec rm -r {} +

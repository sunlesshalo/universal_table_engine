PYTHON ?= python3

.PHONY: install run lint format test docker-build docker-run ui-install ui-build

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

run:
	uvicorn universal_table_engine.app:app --host $${HOST:-0.0.0.0} --port $${PORT:-8000}

lint:
	ruff check universal_table_engine tests

format:
	black universal_table_engine tests
	ruff check universal_table_engine tests --fix
	echo "Format complete"

test:
	pytest -q

docker-build:
	docker build -t universal-table-engine .

docker-run:
	docker run --rm -p 8000:8000 --env-file .env.example -v $$(pwd)/out:/app/out universal-table-engine

ui-install:
	cd ui && npm install

ui-build:
	cd ui && npm run build

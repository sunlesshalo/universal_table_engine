# Universal Table Engine

Universal Table Engine ingests CSV/XLS(X) files, normalizes schemas, and emits a standard JSON structure suitable for downstream analytics. It combines heuristic and LLM-assisted header detection, locale-aware type normalization, semantic aliasing, adapter outputs, and structured logging.

## Features
- Streaming-friendly CSV/XLS(X) ingestion with encoding and delimiter sniffing
- Hybrid header detection (heuristic + optional OpenAI 4o-mini)
- Locale-aware number/date parsing, diacritics cleanup, column deduplication
- Rules-based + LLM semantic aliasing and dataset classification
- JSON, Google Sheets, and BigQuery adapters with feature flags
- Structlog JSON logging, PII detection/masking, FastAPI interface

## Quick Start
```bash
make install
make run
```
Visit `http://localhost:8000/docs` for interactive API docs.

## Configuration
Copy `.env.example` to `.env` and adjust values. Key variables:
- `UTE_ENABLE_LLM=true` and `UTE_LLM_API_KEY` for OpenAI access
- `UTE_ENABLE_SHEETS_ADAPTER` / `UTE_ENABLE_BIGQUERY_ADAPTER` plus credentials for adapters
- `UTE_OUTPUT_DIR` for local JSON exports

## LLM Safety
LLM calls are disabled by default. When enabled, prompts enforce JSON-only responses and fall back to heuristics on failure.

## Testing
```bash
make test
```

## Linting & Formatting
```bash
make lint
make format
```

## Docker
```bash
make docker-build
make docker-run
```

## Example Request
```bash
curl -F "file=@tests/data/messy_header_semicolon.csv" "http://localhost:8000/parse?client_id=demo&adapter=json&enable_llm=false"
```

Adapter outputs are written under `./out/<client_id>/<filename>.json`.

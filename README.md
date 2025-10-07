# Universal Table Engine

Universal Table Engine ingests CSV/XLS(X) files, normalizes schemas, and emits a standard JSON structure suitable for downstream analytics. It combines heuristic and LLM-assisted header detection, locale-aware type normalization, semantic aliasing, adapter outputs, and structured logging.

## Features
- Streaming-friendly CSV/XLS(X) ingestion with encoding and delimiter sniffing
- Hybrid header detection (heuristic + optional OpenAI 4o-mini)
- Locale-aware number/date parsing, diacritics cleanup, column deduplication
- Rules-based + LLM semantic aliasing and dataset classification
- JSON envelope + NDJSON sidecar exports, Google Sheets, and BigQuery adapters with feature flags
- Structlog JSON logging, PII detection/masking, FastAPI interface
- Admin UI upload flow allows overriding header row detection manually when needed
- Authenticated webhook intake with HMAC/API key verification, idempotency cache, and on-disk receipts
- React/Tailwind admin UI for uploads, webhook wizard, deliveries monitor, presets, and settings (served under `/admin`)

## Quick Runbook
```bash
python3 -m venv .venv
source .venv/bin/activate
make install
make run
```
Verify the service:

```bash
curl http://127.0.0.1:8000/health
curl -F "file=@tests/data/messy_header_semicolon.csv" \
  "http://127.0.0.1:8000/parse?client_id=demo&adapter=json&enable_llm=false"
```

Visit `http://localhost:8000/docs` for interactive API docs.

### Admin UI & Webhook Wizard

```
cd ui
npm install
npm run dev        # local dev server at http://localhost:5173
npm run build      # generates ui/dist consumed by FastAPI /admin
```

Once `ui/dist` exists the FastAPI app serves the SPA at `/admin` with static assets under `/admin/assets`.

Webhook intake endpoints:

- `POST /webhook/v1/intake`
- `POST /webhook/v1/intake/{client}`
- `POST /webhook/v1/intake/{client}/{preset}`

Supported modes: multipart (file field), JSON `file_url`, JSON `file_b64`. Auth headers include `Authorization: Bearer <api-key>` and/or `X-UTE-Signature` + `X-UTE-Timestamp` for HMAC. Idempotent requests return stored receipts when the same idempotency key is provided.

Receipts and deliveries endpoints:

- `GET /admin/deliveries`
- `GET /admin/deliveries/{intake_id}`
- `GET /admin/deliveries/{intake_id}/artifacts.zip`
- `POST /admin/deliveries/{intake_id}/replay`
- `GET /admin/presets` / `POST /admin/presets` / `DELETE /admin/presets/{client}/{preset}`
- `GET /admin/settings`

Upload tweaks:

- In the Upload screen you can set `Header row` to a zero-based index to skip auto-detection when you know exactly where the table starts.
- Presets can store the same override (and other defaults) for reuse across uploads/webhooks.

## Configuration
Copy `.env.example` to `.env` and adjust values. Key variables:
- `UTE_ENABLE_LLM=true` and `UTE_LLM_API_KEY` for OpenAI access
- `UTE_ENABLE_SHEETS_ADAPTER` / `UTE_ENABLE_BIGQUERY_ADAPTER` plus credentials for adapters
- `UTE_OUTPUT_DIR` for local JSON exports
- `UTE_JSON_EXPORTS` (default `["envelope","ndjson"]`) to toggle envelope/NDJSON writes, `UTE_JSON_NDJSON_GZIP`, and `UTE_JSON_NDJSON_DROP_NULLS`
- `UTE_BIGQUERY_TIME_PARTITION_FIELD`, `UTE_BIGQUERY_CLUSTER_FIELDS`, and `UTE_BIGQUERY_STRING_FIELDS` to tune BigQuery file loads
- `UTE_BIGQUERY_DEDUP_KEY` to enable optional ROW_NUMBER dedup after each load

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

Zapier webhook sample (replace the URL with your Zap inbound hook):

```bash
curl -X POST "https://hooks.zapier.com/hooks/catch/<zap_id>/<trigger_id>/" \
  -H "Content-Type: application/json" \
  -d @out/demo/messy_header_semicolon.json
```

Adapter outputs are written under `./out/<client_id>/` and include:
- `<filename>.json` (envelope payload)
- `<filename>.ndjson` or `<filename>.ndjson.gz` (rows + metadata header when NDJSON exports are enabled)

### NDJSON Sidecar

When NDJSON exports are enabled the adapter writes a sidecar file with:

- Line 1: metadata envelope, e.g.

  ```json
  {"type":"meta","client_id":"demo","filename":"messy_header_semicolon.csv","rows":42,
   "schema":{"columns":["date","client","amount","paid"]},
   "notes":["rule_applied=demo"],"created_at":"2024-05-21T12:34:56Z",
   "ndjson_version":"1","content_hash":"...","sanitized_columns":{"amount":"amount"}}
  ```
- Lines 2..N: flattened row dictionaries (no wrapper) encoded as UTF-8 with Unix newlines

Set `UTE_JSON_NDJSON_GZIP=true` to write `.ndjson.gz` and `UTE_JSON_NDJSON_DROP_NULLS=true` to omit null-valued columns per row. The `sanitized_columns` map enumerates the BigQuery-safe field names used in the payload, and the `content_hash` hashes the newline-delimited row payload.

### BigQuery file loads

When `/parse?...&adapter=bigquery&use_ndjson_file=true` (or `load_mode=file`) is used, the BigQuery adapter streams the NDJSON file with autodetect enabled. The Admin UI surfaces the resulting job ID, table, and a ready-to-copy command:

```
bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect --compression=GZIP \
  --location=EU project.dataset.table '/abs/path/to/output.ndjson.gz'
```

Partitioning defaults to `created_at` when that column exists and can be overridden with `UTE_BIGQUERY_TIME_PARTITION_FIELD`. When `UTE_BIGQUERY_DEDUP_KEY` is set the adapter reruns the table with a ROW_NUMBER window over the sanitized key so only the latest row per key remains.

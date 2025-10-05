from __future__ import annotations

import re


def test_parse_response_schema(client, data_dir):
    path = data_dir / "messy_header_semicolon.csv"
    with path.open("rb") as handle:
        response = client.post(
            "/parse",
            files={"file": (path.name, handle, "text/csv")},
            params={"client_id": "demo", "adapter": "json", "enable_llm": "false"},
        )
    assert response.status_code == 200
    payload = response.json()
    assert set(payload.keys()) >= {"status", "confidence", "schema", "data", "pii_detected"}
    assert payload["status"] == "ok"
    assert 0.0 <= payload["confidence"] <= 1.0
    assert payload["schema"]["columns"]
    assert payload["schema"]["dataset_type"] in {"financial", "orders", "marketing", "unknown"}
    assert payload["data"], "data rows expected"
    iso_pattern = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:")
    assert iso_pattern.search(payload["data"][0].get("date", ""))
    assert payload["pii_detected"]["phone"] is False

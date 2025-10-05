from __future__ import annotations

import io


def test_health_endpoint(client):
    response = client.get("/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["uptime_seconds"] >= 0


def test_parse_excel_multisheet(client, data_dir):
    path = data_dir / "excel_multisheet.xlsx"
    with path.open("rb") as handle:
        response = client.post(
            "/parse",
            files={"file": (path.name, handle, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            params={"client_id": "excel", "adapter": "json"},
        )
    assert response.status_code == 200
    payload = response.json()
    assert payload["schema"]["dataset_type"] in {"financial", "orders", "unknown"}
    assert payload["data"]


def test_parse_large_csv(client):
    header = "Date;Client;Amount;Paid?\n"
    rows = [
        "01/01/2024;Client {};$1,234.{:02d};Yes\n".format(i, i % 100)
        for i in range(1, 400)
    ]
    buffer = header + "".join(rows)
    stream = io.BytesIO(buffer.encode("utf-8"))
    files = {"file": ("large.csv", stream, "text/csv")}
    response = client.post("/parse", files=files, params={"client_id": "bulk", "adapter": "none"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["data"]
    assert payload["status"] in {"ok", "parsed_with_low_confidence", "needs_rulefile"}
    assert "header_assumed_row" in " ".join(payload["notes"])

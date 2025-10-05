from __future__ import annotations


def test_admin_settings_endpoint(client):
    response = client.get("/admin/settings")
    assert response.status_code == 200
    data = response.json()
    assert "environment" in data
    assert "webhook" in data
    assert "limits" in data


def test_presets_crud_roundtrip(client):
    payload = {
        "client_id": "testclient",
        "preset_id": "baseline",
        "defaults": {
            "adapter": "json",
            "source_hint": "demo",
            "dayfirst": True,
            "decimal_style": "auto",
            "enable_llm": False,
        },
    }
    create = client.post("/admin/presets", json=payload)
    assert create.status_code == 200
    listing = client.get("/admin/presets?client_id=testclient")
    assert listing.status_code == 200
    data = listing.json()
    assert any(item["preset_id"] == "baseline" for item in data)

    delete = client.delete("/admin/presets/testclient/baseline")
    assert delete.status_code == 200
    listing_after = client.get("/admin/presets?client_id=testclient")
    assert listing_after.status_code == 200
    assert all(item["preset_id"] != "baseline" for item in listing_after.json())

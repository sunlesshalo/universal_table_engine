from __future__ import annotations

from pathlib import Path
from typing import Iterator

import pytest
from fastapi.testclient import TestClient

from universal_table_engine.app import app, settings


@pytest.fixture(scope="session", autouse=True)
def configure_settings(tmp_path_factory: pytest.TempPathFactory) -> Iterator[None]:
    output_dir = tmp_path_factory.mktemp("out")
    settings.output_dir = output_dir
    settings.enable_llm = False
    settings.enable_json_adapter = True
    settings.enable_sheets_adapter = False
    settings.enable_bigquery_adapter = False
    yield


@pytest.fixture(scope="session")
def data_dir() -> Path:
    return Path(__file__).parent / "data"


@pytest.fixture(scope="session")
def client() -> TestClient:
    return TestClient(app)

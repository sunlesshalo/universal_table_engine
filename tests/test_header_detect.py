from __future__ import annotations

from universal_table_engine.ingest import file_reader, header_detect
from universal_table_engine.ingest.llm_helper import HeaderPrediction


def test_header_detection_semicolon(data_dir):
    path = data_dir / "messy_header_semicolon.csv"
    sample = file_reader.load_file(
        path.read_bytes(),
        path.name,
        sample_limit=20,
        max_size_bytes=5_000_000,
    )
    result = header_detect.detect_header(sample.sample_rows)
    assert result.header_row >= 0
    assert any("date" in column.lower() for column in result.columns)


def test_header_detection_llm_override(data_dir):
    path = data_dir / "sample_messoric.csv"
    sample = file_reader.load_file(
        path.read_bytes(),
        path.name,
        sample_limit=20,
        max_size_bytes=5_000_000,
    )

    def fake_llm(rows):
        return HeaderPrediction(header_row=3, columns=["c1", "c2"], confidence=0.95)

    result = header_detect.detect_header(sample.sample_rows, llm_client=fake_llm)
    assert result.used_llm is True
    assert result.header_row == 3
    assert result.columns == ["c1", "c2"]


def test_header_detection_low_confidence_flag():
    rows = [["", ""], ["data", "value"], ["", ""]]
    result = header_detect.detect_header(rows)
    assert result.header_row == 1
    assert result.confidence <= 0.95
    assert any(note.startswith("heuristic_header_row=") for note in result.notes)

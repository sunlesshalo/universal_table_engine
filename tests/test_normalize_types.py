from __future__ import annotations

from universal_table_engine.ingest import file_reader, header_detect, normalize, rules_loader
from universal_table_engine.settings import get_settings


def test_normalize_numbers_and_dates(data_dir):
    settings = get_settings()
    path = data_dir / "sample_messoric.csv"
    sample = file_reader.load_file(path.read_bytes(), path.name, sample_limit=20, max_size_bytes=5_000_000)
    header = header_detect.detect_header(sample.sample_rows)
    rules, _ = rules_loader.load_matching_rule(path.name, header.columns, settings=settings)
    result = normalize.normalize_table(
        sample,
        header_row=header.header_row,
        raw_columns=header.columns,
        settings=settings,
        rules=rules,
        llm_aliases=None,
    )
    amount_values = [row["valoare_totala"] for row in result.dataframe.to_dict(orient="records")]
    assert amount_values[0] == 1234.5
    assert "dates_normalized" in result.notes
    assert "decimal_comma_normalized" in result.notes
    assert result.schema["types"]["valoare_totala"] == "number"


def test_boolean_mapping(data_dir):
    settings = get_settings()
    path = data_dir / "messy_header_semicolon.csv"
    sample = file_reader.load_file(path.read_bytes(), path.name, sample_limit=20, max_size_bytes=5_000_000)
    header = header_detect.detect_header(sample.sample_rows)
    result = normalize.normalize_table(
        sample,
        header_row=header.header_row,
        raw_columns=header.columns,
        settings=settings,
        rules=None,
        llm_aliases=None,
    )
    schema_types = result.schema["types"]
    assert schema_types["paid"] == "boolean"
    records = result.dataframe.to_dict(orient="records")
    assert records[0]["paid"] is True
    assert records[1]["paid"] is False

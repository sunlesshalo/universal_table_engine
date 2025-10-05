from __future__ import annotations

import pandas as pd


def fix_ragged_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    max_cols = max(len(row) for row in df.to_numpy())
    if df.shape[1] == max_cols:
        return df
    missing = max_cols - df.shape[1]
    for index in range(missing):
        df[f"extra_{index+1}"] = pd.NA
    return df


def drop_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    return df.dropna(axis=1, how="all")


def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace({pd.NA: None})
    df = df.where(pd.notnull(df), None)
    return df


def ensure_minimum_rows(df: pd.DataFrame, min_rows: int = 1) -> None:
    if len(df.index) < min_rows:
        raise ValueError("dataset contains no rows after normalization")


__all__ = ["fix_ragged_rows", "drop_empty_columns", "sanitize_dataframe", "ensure_minimum_rows"]

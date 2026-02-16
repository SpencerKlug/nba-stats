"""Utilities: string/column normalization, season labels, CLI season resolution."""

from __future__ import annotations

import logging
import re

import pandas as pd

log = logging.getLogger(__name__)


def to_snake_case(s: str) -> str:
    """Convert a string to snake_case (e.g. 'W/L%' -> 'w_l_pct').

    Args:
        s (str): Input string to normalize.

    Returns:
        str: Snake_case string.
    """
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", "_", s.strip()).lower()
    return s or "unknown"


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame columns to snake_case; dedupe with _1, _2 suffix.

    Args:
        df (pd.DataFrame): DataFrame whose columns to normalize.

    Returns:
        pd.DataFrame: DataFrame with normalized column names.
    """
    df = df.copy()
    base = [to_snake_case(str(c)) for c in df.columns]
    seen: dict[str, int] = {}
    out: list[str] = []
    for name in base:
        if name in seen:
            seen[name] += 1
            out.append(f"{name}_{seen[name]}")
        else:
            seen[name] = 0
            out.append(name)
    df.columns = out
    return df


def season_to_label(season: str) -> str:
    """Convert season year to NBA API label (e.g. 2026 -> 2025-26).

    Args:
        season (str): Season year (e.g. 2026 for 2025-26).

    Returns:
        str: NBA API season label (e.g. 2025-26).
    """
    y = int(season)
    return f"{y - 1}-{str(y)[-2:]}"


def align_df_to_existing_columns(df: pd.DataFrame, existing_cols: list[str]) -> pd.DataFrame:
    """Align incoming DataFrame to existing table columns (add NULLs, drop extras).

    Args:
        df (pd.DataFrame): Incoming DataFrame.
        existing_cols (list[str]): Column names of the existing table.

    Returns:
        pd.DataFrame: DataFrame with only existing_cols, missing cols as NULL.
    """
    out = df.copy()
    for c in existing_cols:
        if c not in out.columns:
            out[c] = None
    extra = [c for c in out.columns if c not in existing_cols]
    if extra:
        log.warning("Dropping %d new/unexpected columns: %s", len(extra), ", ".join(extra))
    return out[existing_cols]


def resolve_seasons(
    season: str,
    start_season: str | None,
    end_season: str | None,
) -> list[str]:
    """Resolve season CLI inputs to a sorted inclusive list of season years.

    Args:
        season (str): Default season year (e.g. 2026).
        start_season (str | None): Backfill start year; with end_season gives range.
        end_season (str | None): Backfill end year; with start_season gives range.

    Returns:
        list[str]: Season years, e.g. ['2026'] or ['1997', ..., '2026'].
    """
    if start_season is None and end_season is None:
        return [str(int(season))]

    start = int(start_season or season)
    end = int(end_season or season)
    if start > end:
        raise ValueError(f"start-season ({start}) must be <= end-season ({end})")
    return [str(y) for y in range(start, end + 1)]

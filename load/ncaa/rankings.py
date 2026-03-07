"""NCAA rankings page: fetch and parse /rankings."""

from __future__ import annotations

import pandas as pd

from load.ncaa.core import DIVISION_I, SPORT_CODE_MBB, get, soup


def get_rankings_page(
    division: str = DIVISION_I,
    sport_code: str = SPORT_CODE_MBB,
    academic_year: str | None = None,
) -> str:
    """Fetch the main rankings page for the sport/division (optionally for a given year)."""
    params: dict[str, str] = {
        "division": division,
        "sport_code": sport_code,
    }
    if academic_year:
        params["academic_year"] = str(academic_year)
    return get("/rankings", params=params)


def rankings_tables_to_dfs(html: str) -> dict[str, pd.DataFrame]:
    """Parse rankings page: find all stat category tables, return dict of name -> DataFrame."""
    s = soup(html)
    result: dict[str, pd.DataFrame] = {}
    for i, table in enumerate(s.select("table")):
        rows = table.find_all("tr")
        if not rows:
            continue
        header_cells = rows[0].find_all(["th", "td"])
        headers = [c.get_text(strip=True) for c in header_cells]
        data = []
        for tr in rows[1:]:
            cells = tr.find_all(["td", "th"])
            data.append([c.get_text(strip=True) for c in cells])
        if data:
            ncols = max(len(row) for row in data)
            if len(headers) < ncols:
                headers = headers + [f"col_{j}" for j in range(len(headers), ncols)]
            else:
                headers = headers[:ncols]
            data = [
                row + [""] * (ncols - len(row)) if len(row) < ncols else row[:ncols]
                for row in data
            ]
            df = pd.DataFrame(data, columns=headers)
            result[f"rankings_{i}"] = df
    return result

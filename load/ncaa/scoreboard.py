"""NCAA scoreboard page: fetch and parse /contests/scoreboards."""

from __future__ import annotations

import re

import pandas as pd

from load.ncaa.core import (
    DIVISION_I,
    SPORT_CODE_MBB,
    get,
    soup,
    table_to_df,
)


def get_scoreboard_page(
    division: str = DIVISION_I,
    sport_code: str = SPORT_CODE_MBB,
    academic_year: str | None = None,
    conf_id: str = "-1",
) -> str:
    """Fetch the scoreboard page listing games for the sport/division/year."""
    params: dict[str, str] = {
        "division": division,
        "sport_code": sport_code,
        "conf_id": conf_id,
    }
    if academic_year:
        params["academic_year"] = str(academic_year)
    return get("/contests/scoreboards", params=params)


def parse_scoreboard_to_games(html: str) -> pd.DataFrame:
    """Parse scoreboard page table into games DataFrame (contest_id, game_date, teams, score, etc.)."""
    s = soup(html)
    df = table_to_df(s)
    if df.empty:
        return df
    contest_ids: list[str | None] = []
    for tr in s.select("table tr")[1:]:
        cid = None
        for a in tr.find_all("a", href=True):
            m = re.search(r"/contests/(\d+)(?:/|$|\?)", a.get("href", ""))
            if m:
                cid = m.group(1)
                break
        contest_ids.append(cid)
    if contest_ids:
        df["contest_id"] = contest_ids
    return df

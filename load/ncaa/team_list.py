"""NCAA team list page: fetch and parse /team/inst_team_list."""

from __future__ import annotations

import logging

import pandas as pd

from load.modules import utils
from load.ncaa.core import (
    DIVISION_I,
    SPORT_CODE_MBB,
    academic_year_from_season,
    extract_links,
    get,
    soup,
    table_to_df,
)

log = logging.getLogger(__name__)


def get_team_list_page(
    division: str = DIVISION_I,
    sport_code: str = SPORT_CODE_MBB,
    academic_year: str | None = None,
) -> str:
    """Fetch the team listing page (team index) for the sport/division/year."""
    params: dict[str, str] = {
        "division": division,
        "sport_code": sport_code,
    }
    if academic_year:
        params["academic_year"] = str(academic_year)
    return get("/team/inst_team_list", params=params)


def parse_team_list_html(html: str) -> pd.DataFrame:
    """Parse team list HTML into a DataFrame with team name and link/ID."""
    s = soup(html)
    df = table_to_df(s)
    if df.empty:
        links = extract_links(s, r"org_id=\d+|/team/\d+")
        if links:
            df = pd.DataFrame(links, columns=["team_name", "team_href"])
    return df


def load_team_list(
    season: str,
    division: str = DIVISION_I,
    sport_code: str = SPORT_CODE_MBB,
) -> pd.DataFrame:
    """Load list of teams for the given season (D-I MBB)."""
    academic_year = academic_year_from_season(season)
    log.info("Loading NCAA team list season=%s", season)
    html = get_team_list_page(
        division=division,
        sport_code=sport_code,
        academic_year=academic_year,
    )
    df = parse_team_list_html(html)
    if df.empty:
        log.warning("No teams parsed from team list page")
        return df
    df = utils.normalize_columns(df)
    df["season"] = season
    df["division"] = division
    df["sport_code"] = sport_code
    if "team_href" in df.columns:
        df["org_id"] = df["team_href"].str.extract(r"org_id=(\d+)", expand=False)
    log.info("  team_list: %d rows", len(df))
    return df

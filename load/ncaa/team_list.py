"""NCAA team list page: fetch and parse /team/inst_team_list."""

from __future__ import annotations

import logging

import pandas as pd

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
    # Bronze teams table: college, team_id (unique id from HTML), link
    college = df["team_name"] if "team_name" in df.columns else df.iloc[:, 0]
    link = (
        df["team_href"]
        if "team_href" in df.columns
        else (df.iloc[:, 1] if df.shape[1] > 1 else pd.Series([""] * len(df)))
    )
    # Unique id from href: org_id=123 or /team/123
    if hasattr(link, "str"):
        id_from_org = link.str.extract(r"org_id=(\d+)", expand=False)
        id_from_path = link.str.extract(r"/team/(\d+)", expand=False)
        team_id = id_from_org.fillna(id_from_path)
    else:
        team_id = pd.Series(dtype=object)
    out = pd.DataFrame({"college": college, "team_id": team_id, "link": link})
    log.info("  teams: %d rows", len(out))
    return out

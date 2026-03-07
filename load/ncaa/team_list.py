"""NCAA team list page: fetch and parse /team/inst_team_list."""

from __future__ import annotations

import pandas as pd

from load.ncaa.core import DIVISION_I, SPORT_CODE_MBB, extract_links, get, soup, table_to_df


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

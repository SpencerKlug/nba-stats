"""NCAA scoreboard page: fetch and parse /contests/scoreboards."""

from __future__ import annotations

import logging
import re

import pandas as pd

from load.modules import utils
from load.ncaa import team_list as ncaa_team_list
from load.ncaa import team_season
from load.ncaa.core import (
    DIVISION_I,
    SPORT_CODE_MBB,
    academic_year_from_season,
    get,
    parse_contest_ids_from_html,
    soup,
    table_to_df,
)

log = logging.getLogger(__name__)


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


def load_game_list(
    season: str,
    division: str = DIVISION_I,
    sport_code: str = SPORT_CODE_MBB,
    use_team_schedules: bool = False,
    limit: int | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Load list of games (contest IDs) for the season."""
    academic_year = academic_year_from_season(season)
    log.info("Loading NCAA game list season=%s", season)
    contest_ids: list[str] = []

    if not use_team_schedules:
        html = get_scoreboard_page(
            division=division,
            sport_code=sport_code,
            academic_year=academic_year,
        )
        games_df = parse_scoreboard_to_games(html)
        contest_ids = parse_contest_ids_from_html(html)
        if contest_ids:
            if not games_df.empty:
                games_df = utils.normalize_columns(games_df)
                games_df["season"] = season
                games_df["division"] = division
                games_df["sport_code"] = sport_code
            if limit:
                contest_ids = contest_ids[:limit]
                if not games_df.empty:
                    games_df = games_df.head(limit)
            log.info("  ncaa_schedule: %d games from scoreboard", len(contest_ids))
            return (
                games_df
                if not games_df.empty
                else pd.DataFrame({"contest_id": contest_ids})
            ), contest_ids

    teams = ncaa_team_list.load_team_list(season=season, division=division)
    if teams.empty or "org_id" not in teams.columns:
        log.warning("Cannot load games: no team list or org_id")
        return pd.DataFrame(), []

    org_ids = teams["org_id"].dropna().unique().tolist()
    if limit:
        org_ids = org_ids[: max(1, limit // 50)]
    seen: set[str] = set()
    for i, org_id in enumerate(org_ids):
        html = team_season.get_team_schedule_page(
            org_id=str(org_id), sport_code=sport_code
        )
        for cid in team_season.parse_schedule_contest_ids(html):
            if cid not in seen:
                seen.add(cid)
                contest_ids.append(cid)
        if (i + 1) % 20 == 0:
            log.info(
                "  team schedules: %d/%d teams, %d games",
                i + 1,
                len(org_ids),
                len(contest_ids),
            )
    if limit:
        contest_ids = contest_ids[:limit]
    log.info("  ncaa_schedule: %d games from team schedules", len(contest_ids))
    games_df = pd.DataFrame(
        {
            "contest_id": contest_ids,
            "season": season,
            "division": division,
            "sport_code": sport_code,
        }
    )
    return games_df, contest_ids

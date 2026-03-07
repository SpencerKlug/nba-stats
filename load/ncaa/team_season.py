"""NCAA team index/schedule page: fetch and parse /team/index."""

from __future__ import annotations

from load.ncaa.core import SPORT_CODE_MBB, get, parse_contest_ids_from_html


def get_team_season_page(org_id: str, sport_code: str = SPORT_CODE_MBB) -> str:
    """Fetch a single team's page for the sport (lists seasons/roster/schedule)."""
    return get("/team/index", params={"org_id": org_id, "sport_code": sport_code})


def get_team_schedule_page(org_id: str, sport_code: str = SPORT_CODE_MBB) -> str:
    """Fetch a team's schedule page (lists games for current/default year)."""
    return get("/team/index", params={"org_id": org_id, "sport_code": sport_code})


def parse_schedule_contest_ids(html: str) -> list[str]:
    """Extract contest IDs from a team schedule page."""
    return parse_contest_ids_from_html(html)

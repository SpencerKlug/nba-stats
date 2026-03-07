"""NCAA roster page: fetch /team/roster."""

from __future__ import annotations

from load.ncaa.core import get


def get_team_roster_page(org_id: str, year_id: str) -> str:
    """Fetch roster page for a team for a given year_id (academic year ID from NCAA)."""
    return get("/team/roster", params={"org_id": org_id, "year_id": year_id})

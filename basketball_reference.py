"""
Web scraper for NBA data from Basketball-Reference.com.

Use politely: add delays between requests and respect robots.txt.
Data is for personal/educational use; see site terms of service.
"""

from __future__ import annotations

import io
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment

BASE_URL = "https://www.basketball-reference.com"

# All 30 NBA team abbreviations (for full roster scrape)
NBA_TEAM_ABBREVS = [
    "ATL", "BOS", "BRK", "CHO", "CHI", "CLE", "DAL", "DEN", "DET", "GSW",
    "HOU", "IND", "LAC", "LAL", "MEM", "MIA", "MIL", "MIN", "NOP", "NYK",
    "OKC", "ORL", "PHI", "PHO", "POR", "SAC", "SAS", "TOR", "UTA", "WAS",
]

# Polite headers so the server doesn't reject the request
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": BASE_URL + "/",
}


def get_page(url: str, delay_seconds: float = 1.0) -> requests.Response:
    """
    Fetch a Basketball-Reference page with polite headers and optional delay.

    Args:
        url: Full URL or path (e.g. /leagues/NBA_2025_standings.html).
        delay_seconds: Seconds to wait after the request (rate limiting).

    Returns:
        Response object. Check response.ok before parsing.
    """
    if not url.startswith("http"):
        url = BASE_URL + url
    time.sleep(delay_seconds)
    return requests.get(url, headers=HEADERS, timeout=15)


def _parse_table_from_comment(
    soup: BeautifulSoup, table_id: str | None = None
) -> pd.DataFrame | None:
    """Parse a table that may be inside an HTML comment (common on Basketball-Reference)."""
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    for comment in comments:
        if "table" not in str(comment):
            continue
        comment_soup = BeautifulSoup(comment, "html.parser")
        table = (
            comment_soup.find("table", id=table_id)
            if table_id
            else comment_soup.find("table")
        )
        if table is not None:
            return pd.read_html(io.StringIO(str(table)))[0]
    return None


def _parse_table(
    soup: BeautifulSoup, table_id: str | None = None
) -> pd.DataFrame | None:
    """Get first matching table as a DataFrame, checking both DOM and comments."""
    # Try direct table first
    table = soup.find("table", id=table_id) if table_id else soup.find("table")
    if table is not None:
        return pd.read_html(io.StringIO(str(table)))[0]
    # Fall back to comment-wrapped table
    return _parse_table_from_comment(soup, table_id)


def standings(season: str = "2025") -> pd.DataFrame:
    """
    Get NBA standings for a season.

    Args:
        season: Season year (e.g. "2025" for 2024-25).

    Returns:
        DataFrame with Eastern and Western conference standings.
    """
    # e.g. 2025 -> NBA_2025
    url = f"{BASE_URL}/leagues/NBA_{season}_standings.html"
    resp = get_page(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    # Page has East and West tables (try both possible IDs; some pages use comments)
    east = _parse_table(soup, "confs_standings_E")
    if east is None:
        east = _parse_table(soup, "standings_E")
    west = _parse_table(soup, "confs_standings_W")
    if west is None:
        west = _parse_table(soup, "standings_W")
    if east is not None:
        east["Conference"] = "East"
    if west is not None:
        west["Conference"] = "West"

    if east is not None and west is not None:
        return pd.concat([east, west], ignore_index=True)
    return east if east is not None else west


def season_leaders(season: str = "2025", stat: str = "pts_per_g") -> pd.DataFrame:
    """
    Get season leaders for a given stat category.

    Args:
        season: Season year (e.g. "2025" for 2024-25).
        stat: Stat key (e.g. pts_per_g, trb_per_g, ast_per_g).

    Returns:
        DataFrame of top players for that stat.
    """
    url = f"{BASE_URL}/leagues/NBA_{season}_per_game.html"
    resp = get_page(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    df = _parse_table(soup, "per_game_stats")
    if df is None:
        return pd.DataFrame()
    # Sort by requested stat if present
    if stat in df.columns:
        df = df.sort_values(stat, ascending=False).reset_index(drop=True)
    return df


def team_roster(team_abbrev: str, season: str = "2025") -> pd.DataFrame:
    """
    Get roster for a team in a given season.

    Args:
        team_abbrev: Team abbreviation (e.g. BOS, LAL, GSW).
        season: Season year (e.g. "2025" for 2024-25).

    Returns:
        DataFrame of roster with basic info and stats.
    """
    url = f"{BASE_URL}/teams/{team_abbrev}/{season}.html"
    resp = get_page(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    df = _parse_table(soup, "roster")
    return df if df is not None else pd.DataFrame()


def player_stats_per_game(season: str = "2025") -> pd.DataFrame:
    """
    Get per-game stats for all players in a season.

    Args:
        season: Season year (e.g. "2025" for 2024-25).

    Returns:
        DataFrame of per-game stats (one row per player).
    """
    url = f"{BASE_URL}/leagues/NBA_{season}_per_game.html"
    resp = get_page(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    df = _parse_table(soup, "per_game_stats")
    return df if df is not None else pd.DataFrame()


def player_season_totals(season: str = "2025") -> pd.DataFrame:
    """
    Get raw season totals for all players (counting stats: G, MP, FG, FGA, etc.).
    Use this for the warehouse; derive per-game and rates in dbt.

    Args:
        season: Season year (e.g. "2026" for 2025-26).

    Returns:
        DataFrame with one row per player (per team if traded); raw totals only.
    """
    url = f"{BASE_URL}/leagues/NBA_{season}_totals.html"
    resp = get_page(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    df = _parse_table(soup, "totals_stats")
    if df is None:
        df = _parse_table(soup, "totals")
    return df if df is not None else pd.DataFrame()


def schedule_results(season: str = "2025", month: str | None = None) -> pd.DataFrame:
    """
    Get schedule/results for a season (optionally by month).

    Args:
        season: Season year (e.g. "2025").
        month: Optional month (e.g. "2025-01"). If None, uses full season view.

    Returns:
        DataFrame of games with scores if available.
    """
    if month:
        url = f"{BASE_URL}/leagues/NBA_{season}_games-{month.replace('-', '')}.html"
    else:
        url = f"{BASE_URL}/leagues/NBA_{season}_games.html"
    resp = get_page(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")
    df = _parse_table(soup, "schedule")
    return df if df is not None else pd.DataFrame()

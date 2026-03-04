"""NCAA stats.ncaa.org client: session, HTML parsing, and table extraction.

Men's basketball: sport_code=MBB, division=1. Academic year is the year
the season ends (e.g. 2026 for 2025-26). Pages are HTML with tables;
no public JSON API.
"""

from __future__ import annotations

import logging
import re
import time
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

NCAA_BASE = "https://stats.ncaa.org"
NCAA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:147.0) Gecko/20100101 Firefox/147.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": f"{NCAA_BASE}/",
    "Connection": "keep-alive",
}

# Men's basketball D-I
SPORT_CODE_MBB = "MBB"
DIVISION_I = "1"

# Throttle to be polite
REQUEST_DELAY_SECONDS = 1.0

_session = requests.Session()
_session.headers.update(NCAA_HEADERS)


def _get(path: str, params: dict[str, str] | None = None) -> str:
    """GET a stats.ncaa.org page; returns HTML text. Respects delay."""
    time.sleep(REQUEST_DELAY_SECONDS)
    url = urljoin(NCAA_BASE, path)
    log.debug("GET %s %s", url, params or "")
    resp = _session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.text


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def _table_to_df(soup: BeautifulSoup, table_selector: str = "table") -> pd.DataFrame:
    """Extract the first table on the page into a DataFrame. Uses first row as headers."""
    if table_selector:
        table = soup.select_one(table_selector)
    else:
        table = soup.find("table")
    if table is None:
        return pd.DataFrame()

    rows = table.find_all("tr")
    if not rows:
        return pd.DataFrame()

    def cell_text(cell) -> str:
        return cell.get_text(strip=True) if cell else ""

    header_cells = rows[0].find_all(["th", "td"])
    headers = [cell_text(c) for c in header_cells]
    data = []
    for tr in rows[1:]:
        cells = tr.find_all(["td", "th"])
        data.append([cell_text(c) for c in cells])

    if not data:
        return pd.DataFrame(columns=headers)
    ncols = max(len(row) for row in data)
    # Pad short rows and headers to match max column count (handles colspan, uneven rows)
    if len(headers) < ncols:
        headers = headers + [f"col_{i}" for i in range(len(headers), ncols)]
    else:
        headers = headers[:ncols]
    data = [row + [""] * (ncols - len(row)) if len(row) < ncols else row[:ncols] for row in data]
    return pd.DataFrame(data, columns=headers)


def _extract_links(soup: BeautifulSoup, pattern: str) -> list[tuple[str, str]]:
    """Return list of (link_text, href) for <a> whose href matches pattern."""
    out: list[tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if re.search(pattern, href):
            text = a.get_text(strip=True)
            out.append((text, href))
    return out


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
    return _get("/rankings", params=params)


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
    # Common pattern: /team/inst_team_list or similar
    return _get("/team/inst_team_list", params=params)


def parse_team_list_html(html: str) -> pd.DataFrame:
    """Parse team list HTML into a DataFrame with team name and link/ID.

    Expects a table with team links; extracts text and href. href often
    contains org_id or similar (e.g. /team/index?org_id=580).
    """
    soup = _soup(html)
    df = _table_to_df(soup)
    if df.empty:
        # Fallback: find all team links
        links = _extract_links(soup, r"org_id=\d+|/team/\d+")
        if links:
            df = pd.DataFrame(links, columns=["team_name", "team_href"])
    return df


def get_team_season_page(org_id: str, sport_code: str = SPORT_CODE_MBB) -> str:
    """Fetch a single team's page for the sport (lists seasons/roster/schedule)."""
    return _get("/team/index", params={"org_id": org_id, "sport_code": sport_code})


def get_team_roster_page(org_id: str, year_id: str) -> str:
    """Fetch roster page for a team for a given year_id (academic year ID from NCAA)."""
    return _get("/team/roster", params={"org_id": org_id, "year_id": year_id})


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
    return _get("/contests/scoreboards", params=params)


def parse_contest_ids_from_html(html: str) -> list[str]:
    """Extract contest IDs from any page with links to /contests/{id} or /contests/{id}/."""
    soup = _soup(html)
    ids: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        m = re.search(r"/contests/(\d+)(?:/|$|\?)", a.get("href", ""))
        if m:
            cid = m.group(1)
            if cid not in seen:
                seen.add(cid)
                ids.append(cid)
    return ids


def parse_scoreboard_to_games(html: str) -> pd.DataFrame:
    """Parse scoreboard page table into games DataFrame (contest_id, game_date, teams, score, etc.)."""
    soup = _soup(html)
    df = _table_to_df(soup)
    if df.empty:
        return df
    # Extract contest_id from first link in each row
    contest_ids: list[str | None] = []
    for tr in soup.select("table tr")[1:]:
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


def get_team_schedule_page(org_id: str, sport_code: str = SPORT_CODE_MBB) -> str:
    """Fetch a team's schedule page (lists games for current/default year)."""
    return _get("/team/index", params={"org_id": org_id, "sport_code": sport_code})


def parse_schedule_contest_ids(html: str) -> list[str]:
    """Extract contest IDs from a team schedule page."""
    return parse_contest_ids_from_html(html)


def get_box_score_page(contest_id: str) -> str:
    """Fetch the box score page for a game (player-level stats)."""
    return _get(f"/contests/{contest_id}/box_score")


def parse_box_score_game_info(html: str, contest_id: str) -> dict[str, str | int | None]:
    """Parse game metadata (date, teams, scores) from box score page header."""
    soup = _soup(html)
    info: dict[str, str | int | None] = {
        "contest_id": contest_id,
        "game_date": None,
        "home_team": None,
        "away_team": None,
        "home_score": None,
        "away_score": None,
    }
    for h2 in soup.find_all("h2"):
        text = h2.get_text(strip=True)
        # Common: "Duke 85, North Carolina 78" or "Nov 15, 2024 - Duke 85, North Carolina 78"
        if "-" in text:
            date_part, score_part = text.split("-", 1)
            info["game_date"] = date_part.strip()
            text = score_part.strip()
        m = re.search(r"([\w\s\.\'\-\&]+)\s+(\d+)\s*,\s*([\w\s\.\'\-\&]+)\s+(\d+)", text)
        if m:
            info["away_team"] = m.group(1).strip()
            info["away_score"] = int(m.group(2))
            info["home_team"] = m.group(3).strip()
            info["home_score"] = int(m.group(4))
            break
    return info


def _safe_numeric(val: str) -> str | float | int:
    """Coerce to int/float if numeric; otherwise return string (e.g. '5-12' for FG)."""
    if not val:
        return val
    try:
        if "." in val:
            return float(val)
        return int(val)
    except (ValueError, TypeError):
        return val


def parse_box_score_player_stats(html: str, contest_id: str) -> pd.DataFrame:
    """Parse player-level box score stats from a box score page.

    NCAA box score pages typically have two tables (one per team) with columns
    like Player, MIN, FG-FGA, 3P-3PA, FT-FTA, OREB, DREB, REB, AST, STL, BLK, TO, PF, PTS.
    Returns one DataFrame with all players, with contest_id and team_name columns.
    """
    soup = _soup(html)
    all_rows: list[dict[str, str | float | int]] = []
    for table in soup.select("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        header_cells = rows[0].find_all(["th", "td"])
        headers = [c.get_text(strip=True) for c in header_cells]
        if not any(h.upper() in ("MIN", "PTS", "REB", "FG", "FGM", "FGA") for h in headers):
            continue
        team_name = ""
        for el in [table] + list(table.parents):
            prev = el.find_previous(["h2", "h3", "h4"])
            if prev and prev.get_text(strip=True) and len(prev.get_text(strip=True)) < 80:
                team_name = prev.get_text(strip=True)
                break

        for tr in rows[1:]:
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            vals = [c.get_text(strip=True) for c in cells]
            first_val = vals[0] if vals else ""
            if first_val and first_val.upper() in ("TOTAL", "TOTALS", "TEAM"):
                continue
            row_dict: dict[str, str | float | int] = {
                "contest_id": contest_id,
                "team_name": team_name,
            }
            for i, h in enumerate(headers[: len(vals)]):
                if not h:
                    continue
                key = re.sub(r"[^\w\s]", " ", h).strip().replace(" ", "_").lower()
                if not key:
                    key = f"col_{i}"
                row_dict[key] = _safe_numeric(vals[i])
            all_rows.append(row_dict)

    return pd.DataFrame(all_rows)


def rankings_tables_to_dfs(html: str) -> dict[str, pd.DataFrame]:
    """Parse rankings page: find all stat category tables, return dict of name -> DataFrame."""
    soup = _soup(html)
    result: dict[str, pd.DataFrame] = {}
    for i, table in enumerate(soup.select("table")):
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
            ncols = len(data[0])
            df = pd.DataFrame(data, columns=headers[:ncols])
            result[f"rankings_{i}"] = df
    return result


def html_table_to_df(html: str, table_selector: str = "table") -> pd.DataFrame:
    """Parse first HTML table from page into a DataFrame."""
    soup = _soup(html)
    return _table_to_df(soup, table_selector)

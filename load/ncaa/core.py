"""NCAA stats.ncaa.org core: session, HTTP, and shared HTML/table helpers."""

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


def get(path: str, params: dict[str, str] | None = None) -> str:
    """GET a stats.ncaa.org page; returns HTML text. Respects delay."""
    time.sleep(REQUEST_DELAY_SECONDS)
    url = urljoin(NCAA_BASE, path)
    log.debug("GET %s %s", url, params or "")
    resp = _session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.text


def soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def table_to_df(soup_obj: BeautifulSoup, table_selector: str = "table") -> pd.DataFrame:
    """Extract the first table on the page into a DataFrame. Uses first row as headers."""
    if table_selector:
        table = soup_obj.select_one(table_selector)
    else:
        table = soup_obj.find("table")
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
    data = [
        row + [""] * (ncols - len(row)) if len(row) < ncols else row[:ncols]
        for row in data
    ]
    return pd.DataFrame(data, columns=headers)


def extract_links(soup_obj: BeautifulSoup, pattern: str) -> list[tuple[str, str]]:
    """Return list of (link_text, href) for <a> whose href matches pattern."""
    out: list[tuple[str, str]] = []
    for a in soup_obj.find_all("a", href=True):
        href = a.get("href", "")
        if re.search(pattern, href):
            text = a.get_text(strip=True)
            out.append((text, href))
    return out


def safe_numeric(val: str) -> str | float | int:
    """Coerce to int/float if numeric; otherwise return string (e.g. '5-12' for FG)."""
    if not val:
        return val
    try:
        if "." in val:
            return float(val)
        return int(val)
    except (ValueError, TypeError):
        return val


def parse_contest_ids_from_html(html: str) -> list[str]:
    """Extract contest IDs from any page with links to /contests/{id} or /contests/{id}/."""
    s = soup(html)
    ids: list[str] = []
    seen: set[str] = set()
    for a in s.find_all("a", href=True):
        m = re.search(r"/contests/(\d+)(?:/|$|\?)", a.get("href", ""))
        if m:
            cid = m.group(1)
            if cid not in seen:
                seen.add(cid)
                ids.append(cid)
    return ids


def html_table_to_df(html: str, table_selector: str = "table") -> pd.DataFrame:
    """Parse first HTML table from page into a DataFrame."""
    return table_to_df(soup(html), table_selector)


def academic_year_from_season(season: str) -> str:
    """Map season year (e.g. 2026) to NCAA academic_year (same)."""
    return str(int(season))

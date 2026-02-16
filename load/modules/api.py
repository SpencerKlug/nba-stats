"""NBA stats.nba.com API client: session, retries, and response parsing."""

from __future__ import annotations

import logging
import os
import time

import pandas as pd
import requests
from requests.exceptions import Timeout as RequestsTimeout

log = logging.getLogger(__name__)

STATS_BASE_URL = "https://stats.nba.com/stats"
STATS_HEADERS = {
    "Host": "stats.nba.com",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://stats.nba.com/",
    "Origin": "https://stats.nba.com",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}

RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_RETRIES = int(os.getenv("NBA_API_MAX_RETRIES", "7"))
REQUEST_DELAY_SECONDS = float(os.getenv("NBA_API_REQUEST_DELAY_SECONDS", "1.5"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("NBA_API_TIMEOUT_SECONDS", "60"))
BACKOFF_INITIAL_SECONDS = float(os.getenv("NBA_API_BACKOFF_INITIAL_SECONDS", "2.0"))
BACKOFF_MAX_SECONDS = float(os.getenv("NBA_API_BACKOFF_MAX_SECONDS", "120.0"))

_SESSION = requests.Session()
_SESSION.headers.update(STATS_HEADERS)


def _retry_wait_seconds(attempt: int, resp: requests.Response | None = None) -> float:
    """Compute retry wait time (exponential backoff, optional Retry-After header).

    Args:
        attempt (int): Current attempt index (0-based).
        resp (requests.Response | None, optional): Response from failed request (for Retry-After). Defaults to None.

    Returns:
        float: Seconds to wait before retry.
    """
    backoff = min(BACKOFF_INITIAL_SECONDS * (2**attempt), BACKOFF_MAX_SECONDS)
    if resp is not None:
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                return min(max(float(retry_after), backoff), BACKOFF_MAX_SECONDS)
            except ValueError:
                pass
    return backoff


def call_stats_api(endpoint: str, params: dict[str, str]) -> dict:
    """Call a stats.nba.com endpoint with retries and exponential backoff.

    Args:
        endpoint (str): API endpoint path (e.g. leaguegamelog).
        params (dict[str, str]): Query parameters for the request.

    Returns:
        dict: JSON response body.
    """
    url = f"{STATS_BASE_URL}/{endpoint}"
    time.sleep(REQUEST_DELAY_SECONDS)
    for attempt in range(MAX_RETRIES + 1):
        log.info("GET %s attempt=%d/%d", endpoint, attempt + 1, MAX_RETRIES + 1)
        try:
            resp = _SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        except RequestsTimeout:
            if attempt < MAX_RETRIES:
                wait = _retry_wait_seconds(attempt)
                log.warning(
                    "timeout endpoint=%s retrying in %.1fs",
                    endpoint,
                    wait,
                )
                time.sleep(wait)
                continue
            raise
        if resp.status_code in RETRY_STATUS_CODES:
            if attempt < MAX_RETRIES:
                wait = _retry_wait_seconds(attempt, resp)
                log.warning(
                    "status=%s endpoint=%s retrying in %.1fs",
                    resp.status_code,
                    endpoint,
                    wait,
                )
                time.sleep(wait)
                continue
            resp.raise_for_status()
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Failed to fetch endpoint={endpoint}")


def resultset_to_df(payload: dict, name: str | None = None, index: int = 0) -> pd.DataFrame:
    """Parse NBA stats API resultSet(s) JSON into a DataFrame.

    Args:
        payload (dict): API response JSON.
        name (str | None, optional): Result set name to select. Defaults to None.
        index (int, optional): Index of result set when name not used. Defaults to 0.

    Returns:
        pd.DataFrame: Parsed table; empty if no matching result set.
    """
    if "resultSets" in payload:
        sets = payload["resultSets"]
        if isinstance(sets, dict):
            headers = sets.get("headers", [])
            rows = sets.get("rowSet", [])
            return pd.DataFrame(rows, columns=headers)
        if name is not None:
            for rs in sets:
                if rs.get("name") == name:
                    return pd.DataFrame(rs.get("rowSet", []), columns=rs.get("headers", []))
        rs = sets[index]
        return pd.DataFrame(rs.get("rowSet", []), columns=rs.get("headers", []))

    if "resultSet" in payload:
        rs = payload["resultSet"]
        return pd.DataFrame(rs.get("rowSet", []), columns=rs.get("headers", []))

    return pd.DataFrame()

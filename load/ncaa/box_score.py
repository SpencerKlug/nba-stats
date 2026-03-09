"""NCAA box score page: fetch and parse /contests/{id}/box_score."""

from __future__ import annotations

import logging
import re

import pandas as pd

from load.modules import utils
from load.ncaa.core import get, safe_numeric, soup

log = logging.getLogger(__name__)


def get_box_score_page(contest_id: str) -> str:
    """Fetch the box score page for a game (player-level stats)."""
    return get(f"/contests/{contest_id}/box_score")


def parse_box_score_game_info(
    html: str, contest_id: str
) -> dict[str, str | int | None]:
    """Parse game metadata (date, teams, scores) from box score page header."""
    s = soup(html)
    info: dict[str, str | int | None] = {
        "contest_id": contest_id,
        "game_date": None,
        "home_team": None,
        "away_team": None,
        "home_score": None,
        "away_score": None,
    }
    for h2 in s.find_all("h2"):
        text = h2.get_text(strip=True)
        if "-" in text:
            date_part, score_part = text.split("-", 1)
            info["game_date"] = date_part.strip()
            text = score_part.strip()
        m = re.search(
            r"([\w\s\.\'\-\&]+)\s+(\d+)\s*,\s*([\w\s\.\'\-\&]+)\s+(\d+)", text
        )
        if m:
            info["away_team"] = m.group(1).strip()
            info["away_score"] = int(m.group(2))
            info["home_team"] = m.group(3).strip()
            info["home_score"] = int(m.group(4))
            break
    return info


def parse_box_score_player_stats(html: str, contest_id: str) -> pd.DataFrame:
    """Parse player-level box score stats from a box score page."""
    s = soup(html)
    all_rows: list[dict[str, str | float | int]] = []
    for table in s.select("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        header_cells = rows[0].find_all(["th", "td"])
        headers = [c.get_text(strip=True) for c in header_cells]
        if not any(
            h.upper() in ("MIN", "PTS", "REB", "FG", "FGM", "FGA") for h in headers
        ):
            continue
        team_name = ""
        for el in [table] + list(table.parents):
            prev = el.find_previous(["h2", "h3", "h4"])
            if (
                prev
                and prev.get_text(strip=True)
                and len(prev.get_text(strip=True)) < 80
            ):
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
                row_dict[key] = safe_numeric(vals[i])
            all_rows.append(row_dict)

    return pd.DataFrame(all_rows)


def load_player_box_scores_and_schedule(
    contest_ids: list[str],
    season: str,
    limit: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load player-level box scores and game metadata. One fetch per game."""
    if limit:
        contest_ids = contest_ids[:limit]
    log.info("Loading NCAA box scores and schedule for %d games", len(contest_ids))
    player_frames: list[pd.DataFrame] = []
    game_rows: list[dict] = []
    for i, cid in enumerate(contest_ids):
        try:
            html = get_box_score_page(cid)
            df = parse_box_score_player_stats(html, cid)
            if not df.empty:
                df["season"] = season
                player_frames.append(df)
            info = parse_box_score_game_info(html, cid)
            info["season"] = season
            game_rows.append(info)
        except Exception as e:
            log.warning("box score contest_id=%s: %s", cid, e)
        if (i + 1) % 50 == 0:
            log.info(
                "  box scores: %d/%d games, %d player-rows",
                i + 1,
                len(contest_ids),
                sum(len(f) for f in player_frames),
            )
    player_df = (
        utils.normalize_columns(pd.concat(player_frames, ignore_index=True))
        if player_frames
        else pd.DataFrame()
    )
    schedule_df = (
        utils.normalize_columns(pd.DataFrame(game_rows))
        if game_rows
        else pd.DataFrame()
    )
    log.info(
        "  player_box_scores: %d rows, schedule: %d games",
        len(player_df),
        len(schedule_df),
    )
    return player_df, schedule_df

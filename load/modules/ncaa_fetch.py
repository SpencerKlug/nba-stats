"""Fetch raw NCAA men's basketball data from stats.ncaa.org into DataFrames.

Academic year = season end year (e.g. 2026 for 2025-26). All data is D-I MBB
unless overridden. Outputs are normalized to snake_case and include season.
"""

from __future__ import annotations

import logging

import pandas as pd

from load.modules import ncaa_client, utils

log = logging.getLogger(__name__)


def _academic_year_from_season(season: str) -> str:
    """Map season year (e.g. 2026) to NCAA academic_year (same)."""
    return str(int(season))


def load_rankings(
    season: str,
    division: str = ncaa_client.DIVISION_I,
    sport_code: str = ncaa_client.SPORT_CODE_MBB,
) -> dict[str, pd.DataFrame]:
    """Load rankings page and return all stat tables as DataFrames.

    Args:
        season: Season end year (e.g. 2026 for 2025-26).
        division: NCAA division (default D-I).
        sport_code: Sport code (default MBB).

    Returns:
        Dict of table name -> DataFrame (columns normalized, season added).
    """
    academic_year = _academic_year_from_season(season)
    log.info("Loading NCAA rankings season=%s division=%s sport=%s", season, division, sport_code)
    html = ncaa_client.get_rankings_page(
        division=division,
        sport_code=sport_code,
        academic_year=academic_year,
    )
    tables = ncaa_client.rankings_tables_to_dfs(html)
    out: dict[str, pd.DataFrame] = {}
    for name, df in tables.items():
        if df.empty:
            continue
        df = utils.normalize_columns(df)
        df["season"] = season
        df["division"] = division
        df["sport_code"] = sport_code
        out[name] = df
    log.info("  rankings: %d tables", len(out))
    return out


def load_team_list(
    season: str,
    division: str = ncaa_client.DIVISION_I,
    sport_code: str = ncaa_client.SPORT_CODE_MBB,
) -> pd.DataFrame:
    """Load list of teams for the given season (D-I MBB).

    Returns DataFrame with team name, link/href, and optionally org_id if parseable.
    """
    academic_year = _academic_year_from_season(season)
    log.info("Loading NCAA team list season=%s", season)
    html = ncaa_client.get_team_list_page(
        division=division,
        sport_code=sport_code,
        academic_year=academic_year,
    )
    df = ncaa_client.parse_team_list_html(html)
    if df.empty:
        log.warning("No teams parsed from team list page")
        return df
    df = utils.normalize_columns(df)
    df["season"] = season
    df["division"] = division
    df["sport_code"] = sport_code
    # Extract org_id from href if present (e.g. org_id=580)
    if "team_href" in df.columns:
        df["org_id"] = df["team_href"].str.extract(r"org_id=(\d+)", expand=False)
    log.info("  team_list: %d rows", len(df))
    return df


def load_team_rankings_single_table(
    season: str,
    division: str = ncaa_client.DIVISION_I,
    sport_code: str = ncaa_client.SPORT_CODE_MBB,
) -> pd.DataFrame:
    """Load the first/main rankings table from the rankings page as one DataFrame.

    Convenience when you want a single table (e.g. team scoring offense).
    """
    tables = load_rankings(season=season, division=division, sport_code=sport_code)
    if not tables:
        return pd.DataFrame()
    # Return first table; caller can choose by key if needed
    first_key = next(iter(tables))
    return tables[first_key]


def load_game_list(
    season: str,
    division: str = ncaa_client.DIVISION_I,
    sport_code: str = ncaa_client.SPORT_CODE_MBB,
    use_team_schedules: bool = False,
    limit: int | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Load list of games (contest IDs) for the season.

    Tries scoreboard first; if empty, falls back to iterating team schedules.
    Returns (games_df with contest_id and metadata, list of contest_ids).
    """
    academic_year = _academic_year_from_season(season)
    log.info("Loading NCAA game list season=%s", season)

    contest_ids: list[str] = []

    if not use_team_schedules:
        html = ncaa_client.get_scoreboard_page(
            division=division,
            sport_code=sport_code,
            academic_year=academic_year,
        )
        games_df = ncaa_client.parse_scoreboard_to_games(html)
        contest_ids = ncaa_client.parse_contest_ids_from_html(html)
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
            return games_df if not games_df.empty else pd.DataFrame({"contest_id": contest_ids}), contest_ids

    # Fallback: team schedules
    team_list = load_team_list(season=season, division=division)
    if team_list.empty or "org_id" not in team_list.columns:
        log.warning("Cannot load games: no team list or org_id")
        return pd.DataFrame(), []

    org_ids = team_list["org_id"].dropna().unique().tolist()
    if limit:
        org_ids = org_ids[: max(1, limit // 50)]
    seen: set[str] = set()
    for i, org_id in enumerate(org_ids):
        html = ncaa_client.get_team_schedule_page(org_id=str(org_id), sport_code=sport_code)
        for cid in ncaa_client.parse_schedule_contest_ids(html):
            if cid not in seen:
                seen.add(cid)
                contest_ids.append(cid)
        if (i + 1) % 20 == 0:
            log.info("  team schedules: %d/%d teams, %d games", i + 1, len(org_ids), len(contest_ids))
    if limit:
        contest_ids = contest_ids[: limit]
    log.info("  ncaa_schedule: %d games from team schedules", len(contest_ids))
    games_df = pd.DataFrame({"contest_id": contest_ids, "season": season, "division": division, "sport_code": sport_code})
    return games_df, contest_ids


def load_player_box_scores_and_schedule(
    contest_ids: list[str],
    season: str,
    limit: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load player-level box scores and game metadata. One fetch per game.
    Returns (player_box_scores_df, schedule_df).
    """
    if limit:
        contest_ids = contest_ids[: limit]
    log.info("Loading NCAA box scores and schedule for %d games", len(contest_ids))
    player_frames: list[pd.DataFrame] = []
    game_rows: list[dict] = []
    for i, cid in enumerate(contest_ids):
        try:
            html = ncaa_client.get_box_score_page(cid)
            df = ncaa_client.parse_box_score_player_stats(html, cid)
            if not df.empty:
                df["season"] = season
                player_frames.append(df)
            info = ncaa_client.parse_box_score_game_info(html, cid)
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
    schedule_df = utils.normalize_columns(pd.DataFrame(game_rows)) if game_rows else pd.DataFrame()
    log.info("  ncaa_player_box_scores: %d rows, ncaa_schedule: %d games", len(player_df), len(schedule_df))
    return player_df, schedule_df


def load_ncaa_mbb_season(
    season: str,
    division: str = ncaa_client.DIVISION_I,
    include_team_list: bool = True,
    include_rankings: bool = True,
    include_games: bool = True,
    include_box_scores: bool = True,
    use_team_schedules: bool = False,
    limit: int | None = None,
) -> dict[str, pd.DataFrame]:
    """Load one season of NCAA men's basketball data into raw tables.

    Args:
        season: Season end year (e.g. 2026 for 2025-26).
        division: NCAA division (default D-I).
        include_team_list: Fetch team list.
        include_rankings: Fetch rankings page tables.
        include_games: Fetch game schedule.
        include_box_scores: Fetch player-level box scores for each game.
        use_team_schedules: Use team schedules instead of scoreboard for game list.
        limit: Cap games/teams for testing.

    Returns:
        Dict of table_name -> DataFrame.
    """
    result: dict[str, pd.DataFrame] = {}

    if include_team_list:
        team_list = load_team_list(season=season, division=division)
        if not team_list.empty:
            result["ncaa_team_list"] = team_list

    if include_rankings:
        rankings = load_rankings(season=season, division=division)
        for name, df in rankings.items():
            result[name] = df

    if include_games or include_box_scores:
        games_df, contest_ids = load_game_list(
            season=season,
            division=division,
            use_team_schedules=use_team_schedules,
            limit=limit,
        )
        if contest_ids:
            if include_box_scores:
                box_df, schedule_from_box = load_player_box_scores_and_schedule(
                    contest_ids, season, limit=limit
                )
                if not box_df.empty:
                    result["ncaa_player_box_scores"] = box_df
                if include_games and not schedule_from_box.empty:
                    result["ncaa_schedule"] = schedule_from_box
            elif include_games:
                # Games only (no box scores): use scoreboard/team-schedule data
                if not games_df.empty:
                    result["ncaa_schedule"] = games_df

    return result

"""Fetch raw NBA data from stats.nba.com: game logs, rosters, schedule, and dimensions."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp
import pandas as pd

from load.modules import api, utils

log = logging.getLogger(__name__)


def _game_date_for_api(d: str) -> str:
    """Convert game_date to NBA API format (MM/DD/YYYY)."""
    try:
        dt = pd.to_datetime(d)
        return dt.strftime("%m/%d/%Y")
    except Exception:
        return str(d)


def load_team_game_logs(season: str, season_type: str = "Regular Season") -> pd.DataFrame:
    """Load team game logs from stats.nba.com API.

    Args:
        season (str): Season year (e.g. 2026 for 2025-26)
        season_type (str, optional): NBA API season type. Defaults to "Regular Season".

    Returns:
        pd.DataFrame: Team game logs DataFrame
    """
    season_label = utils.season_to_label(season)
    log.info("Loading team game logs season=%s (%s)", season_label, season_type)
    payload = api.call_stats_api(
        "leaguegamelog",
        {
            "Counter": "1000",
            "Direction": "DESC",
            "LeagueID": "00",
            "PlayerOrTeam": "T",
            "Season": season_label,
            "SeasonType": season_type,
            "Sorter": "DATE",
        },
    )
    df = api.resultset_to_df(payload)
    if df.empty:
        log.warning("No team game logs returned")
        return df
    df = utils.normalize_columns(df)
    df["season"] = season
    df["season_label"] = season_label
    df["season_type"] = season_type
    log.info("  team_game_logs: %d rows", len(df))
    return df


def load_player_game_logs(season: str, season_type: str = "Regular Season") -> pd.DataFrame:
    """Load player game logs from stats.nba.com API.

    Args:
        season (str): Season year (e.g. 2026 for 2025-26).
        season_type (str, optional): NBA API season type. Defaults to "Regular Season".

    Returns:
        pd.DataFrame: Player game logs DataFrame.
    """
    season_label = utils.season_to_label(season)
    log.info("Loading player game logs season=%s (%s)", season_label, season_type)
    payload = api.call_stats_api(
        "leaguegamelog",
        {
            "Counter": "1000",
            "Direction": "DESC",
            "LeagueID": "00",
            "PlayerOrTeam": "P",
            "Season": season_label,
            "SeasonType": season_type,
            "Sorter": "DATE",
        },
    )
    df = api.resultset_to_df(payload)
    if df.empty:
        log.warning("No player game logs returned")
        return df
    df = utils.normalize_columns(df)
    df["season"] = season
    df["season_label"] = season_label
    df["season_type"] = season_type
    log.info("  player_game_logs: %d rows", len(df))
    return df


def load_team_rosters(season: str, team_game_logs: pd.DataFrame) -> pd.DataFrame:
    """Load commonteamroster for each team id observed in team game logs.

    Args:
        season (str): Season year (e.g. 2026 for 2025-26).
        team_game_logs (pd.DataFrame): Team game logs used to derive team IDs.

    Returns:
        pd.DataFrame: Roster rows (one per player-team-season); empty if no teams.
    """
    if team_game_logs.empty or "team_id" not in team_game_logs.columns:
        log.warning("Cannot load rosters: team_game_logs empty or missing team_id")
        return pd.DataFrame()

    season_label = utils.season_to_label(season)
    teams = (
        team_game_logs[["team_id", "team_abbreviation"]]
        .drop_duplicates()
        .sort_values("team_abbreviation")
    )

    frames: list[pd.DataFrame] = []
    log.info("Loading rosters for %d teams", len(teams))

    for i, row in enumerate(teams.itertuples(index=False), 1):
        team_id = int(row.team_id)
        team_abbrev = str(row.team_abbreviation)
        payload = api.call_stats_api(
            "commonteamroster",
            {"LeagueID": "00", "Season": season_label, "TeamID": str(team_id)},
        )
        df = api.resultset_to_df(payload, name="CommonTeamRoster")
        if df.empty:
            log.warning("  %s (%s): empty roster", team_abbrev, team_id)
            continue
        df = utils.normalize_columns(df)
        df["team_id"] = team_id
        df["team_abbreviation"] = team_abbrev
        df["season"] = season
        df["season_label"] = season_label
        frames.append(df)
        log.info(
            "  %s (%s): %d players (%d/%d)",
            team_abbrev,
            team_id,
            len(df),
            i,
            len(teams),
        )

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    log.info("  team_rosters: %d rows", len(out))
    return out


def load_common_all_players(season: str) -> pd.DataFrame:
    """Load commonallplayers: master player list for a season.

    Args:
        season (str): Season year (e.g. 2026 for 2025-26).

    Returns:
        pd.DataFrame: One row per player active in the season.
    """
    season_label = utils.season_to_label(season)
    log.info("Loading commonallplayers season=%s", season_label)
    payload = api.call_stats_api(
        "commonallplayers",
        {
            "LeagueID": "00",
            "Season": season_label,
            "IsOnlyCurrentSeason": "1",
        },
    )
    df = api.resultset_to_df(payload, name="CommonAllPlayers")
    if df.empty:
        log.warning("No commonallplayers returned")
        return df
    df = utils.normalize_columns(df)
    df["season"] = season
    df["season_label"] = season_label
    log.info("  common_all_players: %d rows", len(df))
    return df


def load_scoreboard(
    game_date: str, season: str, season_type: str = "Regular Season"
) -> pd.DataFrame:
    """Load scoreboard for a single date (GameHeader = one row per game).

    Args:
        game_date (str): Date in any parseable format (e.g. 2024-01-15).
        season (str): Season year for tagging.
        season_type (str): NBA API season type.

    Returns:
        pd.DataFrame: One row per game (schedule metadata).
    """
    api_date = _game_date_for_api(game_date)
    payload = api.call_stats_api(
        "scoreboardv2",
        {"LeagueID": "00", "GameDate": api_date, "DayOffset": "0"},
    )
    df = api.resultset_to_df(payload, name="GameHeader")
    if df.empty:
        return df
    df = utils.normalize_columns(df)
    df["game_date_api"] = api_date
    df["season"] = season
    df["season_type"] = season_type
    return df


def load_schedule(
    team_game_logs: pd.DataFrame,
    season: str,
    season_type: str = "Regular Season",
) -> pd.DataFrame:
    """Load schedule (GameHeader) for all unique game dates in team_game_logs.

    Args:
        team_game_logs (pd.DataFrame): Team game logs to derive dates.
        season (str): Season year.
        season_type (str): NBA API season type.

    Returns:
        pd.DataFrame: One row per game (schedule metadata).
    """
    if team_game_logs.empty or "game_date" not in team_game_logs.columns:
        log.warning("Cannot load schedule: team_game_logs empty or missing game_date")
        return pd.DataFrame()

    dates = team_game_logs["game_date"].dropna().unique().tolist()
    log.info("Loading schedule for %d unique dates", len(dates))

    frames: list[pd.DataFrame] = []
    for i, d in enumerate(sorted(dates), 1):
        df = load_scoreboard(str(d), season=season, season_type=season_type)
        if not df.empty:
            frames.append(df)
        if i % 30 == 0:
            log.info("  schedule: %d/%d dates", i, len(dates))

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["game_id"])
    log.info("  schedule: %d games", len(out))
    return out


def load_box_score_summary(
    game_id: str, season: str, season_type: str = "Regular Season"
) -> pd.DataFrame:
    """Load boxscoresummaryv2 GameSummary for a single game.

    Args:
        game_id (str): 10-digit game ID (e.g. 0022500001).
        season (str): Season year for tagging.

    Returns:
        pd.DataFrame: One row with game summary (arena, officials, etc.).
    """
    payload = api.call_stats_api(
        "boxscoresummaryv2",
        {
            "GameID": str(game_id),
            "StartPeriod": "0",
            "EndPeriod": "14",
            "StartRange": "0",
            "EndRange": "2147483647",
            "RangeType": "0",
        },
    )
    df = api.resultset_to_df(payload, name="GameSummary")
    if df.empty:
        return df
    df = utils.normalize_columns(df)
    df["season"] = season
    df["season_type"] = season_type
    return df


def load_box_score_summaries(
    team_game_logs: pd.DataFrame,
    season: str,
    season_type: str = "Regular Season",
) -> pd.DataFrame:
    """Load box score summaries for all unique games in team_game_logs.

    Args:
        team_game_logs (pd.DataFrame): Team game logs to derive game IDs.
        season (str): Season year.
        season_type (str): NBA API season type.

    Returns:
        pd.DataFrame: One row per game (arena, officials, attendance, etc.).
    """
    if team_game_logs.empty or "game_id" not in team_game_logs.columns:
        log.warning("Cannot load box summaries: team_game_logs empty or missing game_id")
        return pd.DataFrame()

    game_ids = team_game_logs["game_id"].dropna().astype(str).str.strip().unique().tolist()
    log.info("Loading box score summaries for %d games", len(game_ids))

    frames: list[pd.DataFrame] = []
    for i, gid in enumerate(game_ids, 1):
        df = load_box_score_summary(gid, season=season, season_type=season_type)
        if not df.empty:
            frames.append(df)
        if i % 50 == 0:
            log.info("  box_summaries: %d/%d games", i, len(game_ids))

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    log.info("  box_summaries: %d rows", len(out))
    return out


def load_common_player_info(player_id: int | str, season: str) -> pd.DataFrame:
    """Load commonplayerinfo for a single player (bio: height, weight, school, draft).

    Args:
        player_id (int | str): NBA player ID.
        season (str): Season year for tagging.

    Returns:
        pd.DataFrame: One row with player bio.
    """
    payload = api.call_stats_api(
        "commonplayerinfo",
        {"LeagueID": "00", "PlayerID": str(player_id)},
    )
    df = api.resultset_to_df(payload, name="CommonPlayerInfo")
    if df.empty:
        return df
    df = utils.normalize_columns(df)
    df["season"] = season
    return df


def load_player_info(common_all_players: pd.DataFrame, season: str) -> pd.DataFrame:
    """Load commonplayerinfo for all players in common_all_players.

    Args:
        common_all_players (pd.DataFrame): Output of load_common_all_players.
        season (str): Season year.

    Returns:
        pd.DataFrame: One row per player (bio details).
    """
    if common_all_players.empty or "person_id" not in common_all_players.columns:
        log.warning("Cannot load player info: common_all_players empty or missing person_id")
        return pd.DataFrame()

    player_ids = common_all_players["person_id"].dropna().unique().tolist()
    log.info("Loading player info for %d players", len(player_ids))

    frames: list[pd.DataFrame] = []
    for i, pid in enumerate(player_ids, 1):
        df = load_common_player_info(pid, season=season)
        if not df.empty:
            frames.append(df)
        if i % 50 == 0:
            log.info("  player_info: %d/%d players", i, len(player_ids))

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    log.info("  player_info: %d rows", len(out))
    return out


async def _load_all_raw_async(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    season: str,
    season_type: str,
) -> dict[str, pd.DataFrame]:
    """Async implementation: fetch all raw tables with 3 concurrent workers."""
    season_label = utils.season_to_label(season)

    async def one(endpoint: str, params: dict[str, str]) -> dict:
        return await api.call_stats_api_async(session, semaphore, endpoint, params)

    def to_df(payload: dict, name: str | None = None, index: int = 0) -> pd.DataFrame:
        return api.resultset_to_df(payload, name=name, index=index)

    # Phase 1: initial bulk endpoints (3 parallel)
    log.info("Fetching raw NBA stats data for season=%s season_type=%s", season, season_type)
    log.info("Loading team game logs, player game logs, commonallplayers (3 parallel)")
    team_payload, player_payload, common_payload = await asyncio.gather(
        one(
            "leaguegamelog",
            {
                "Counter": "1000",
                "Direction": "DESC",
                "LeagueID": "00",
                "PlayerOrTeam": "T",
                "Season": season_label,
                "SeasonType": season_type,
                "Sorter": "DATE",
            },
        ),
        one(
            "leaguegamelog",
            {
                "Counter": "1000",
                "Direction": "DESC",
                "LeagueID": "00",
                "PlayerOrTeam": "P",
                "Season": season_label,
                "SeasonType": season_type,
                "Sorter": "DATE",
            },
        ),
        one(
            "commonallplayers",
            {"LeagueID": "00", "Season": season_label, "IsOnlyCurrentSeason": "1"},
        ),
    )

    team_logs = to_df(team_payload)
    if not team_logs.empty:
        team_logs = utils.normalize_columns(team_logs)
        team_logs["season"] = season
        team_logs["season_label"] = season_label
        team_logs["season_type"] = season_type
    log.info("  team_game_logs: %d rows", len(team_logs))

    player_logs = to_df(player_payload)
    if not player_logs.empty:
        player_logs = utils.normalize_columns(player_logs)
        player_logs["season"] = season
        player_logs["season_label"] = season_label
        player_logs["season_type"] = season_type
    log.info("  player_game_logs: %d rows", len(player_logs))

    common_players = to_df(common_payload, name="CommonAllPlayers")
    if not common_players.empty:
        common_players = utils.normalize_columns(common_players)
        common_players["season"] = season
        common_players["season_label"] = season_label
    log.info("  common_all_players: %d rows", len(common_players))

    # Phase 2: rosters and schedule (parallel, each uses semaphore)
    if team_logs.empty or "team_id" not in team_logs.columns:
        rosters = pd.DataFrame()
        schedule = pd.DataFrame()
    else:
        teams = (
            team_logs[["team_id", "team_abbreviation"]]
            .drop_duplicates()
            .sort_values("team_abbreviation")
        )
        dates = team_logs["game_date"].dropna().unique().tolist()
        log.info("Loading rosters for %d teams", len(teams))
        log.info("Loading schedule for %d unique dates", len(dates))

        async def fetch_roster(row: Any) -> pd.DataFrame:
            team_id = int(row.team_id)
            team_abbrev = str(row.team_abbreviation)
            try:
                p = await one(
                    "commonteamroster",
                    {"LeagueID": "00", "Season": season_label, "TeamID": str(team_id)},
                )
                df = to_df(p, name="CommonTeamRoster")
                if df.empty:
                    return pd.DataFrame()
                df = utils.normalize_columns(df)
                df["team_id"] = team_id
                df["team_abbreviation"] = team_abbrev
                df["season"] = season
                df["season_label"] = season_label
                return df
            except Exception as e:
                log.warning("Skipping roster team_id=%s (%s): %s", team_id, team_abbrev, e)
                return pd.DataFrame()

        async def fetch_scoreboard(d: str) -> pd.DataFrame:
            api_date = _game_date_for_api(d)
            try:
                p = await one(
                    "scoreboardv2",
                    {"LeagueID": "00", "GameDate": api_date, "DayOffset": "0"},
                )
                df = to_df(p, name="GameHeader")
                if df.empty:
                    return df
                df = utils.normalize_columns(df)
                df["game_date_api"] = api_date
                df["season"] = season
                df["season_type"] = season_type
                return df
            except Exception as e:
                log.warning("Skipping scoreboard date=%s: %s", d, e)
                return pd.DataFrame()

        roster_dfs = await asyncio.gather(
            *[fetch_roster(row) for row in teams.itertuples(index=False)]
        )
        schedule_dfs = await asyncio.gather(*[fetch_scoreboard(str(d)) for d in sorted(dates)])

        roster_list = [d for d in roster_dfs if not d.empty]
        schedule_list = [d for d in schedule_dfs if not d.empty]
        rosters = pd.concat(roster_list, ignore_index=True) if roster_list else pd.DataFrame()
        schedule = pd.concat(schedule_list, ignore_index=True) if schedule_list else pd.DataFrame()
        schedule = schedule.drop_duplicates(subset=["game_id"])
        log.info("  team_rosters: %d rows", len(rosters))
        log.info("  schedule: %d games", len(schedule))

    # Phase 3: box summaries and player info (parallel)
    if team_logs.empty or "game_id" not in team_logs.columns:
        box_summaries = pd.DataFrame()
    else:
        game_ids = team_logs["game_id"].dropna().astype(str).str.strip().unique().tolist()
        log.info("Loading box score summaries for %d games", len(game_ids))

        async def fetch_box(gid: str) -> pd.DataFrame:
            try:
                p = await one(
                    "boxscoresummaryv2",
                    {
                        "GameID": str(gid),
                        "StartPeriod": "0",
                        "EndPeriod": "14",
                        "StartRange": "0",
                        "EndRange": "2147483647",
                        "RangeType": "0",
                    },
                )
                df = to_df(p, name="GameSummary")
                if df.empty:
                    return df
                df = utils.normalize_columns(df)
                df["season"] = season
                df["season_type"] = season_type
                return df
            except Exception as e:
                log.warning("Skipping box summary game_id=%s: %s", gid, e)
                return pd.DataFrame()

        box_dfs = await asyncio.gather(*[fetch_box(gid) for gid in game_ids])
        box_list = [d for d in box_dfs if not d.empty]
        box_summaries = pd.concat(box_list, ignore_index=True) if box_list else pd.DataFrame()
        log.info("  box_summaries: %d rows", len(box_summaries))

    if common_players.empty or "person_id" not in common_players.columns:
        player_info = pd.DataFrame()
    else:
        player_ids = common_players["person_id"].dropna().unique().tolist()
        log.info("Loading player info for %d players", len(player_ids))

        async def fetch_player_info(pid: Any) -> pd.DataFrame:
            try:
                p = await one(
                    "commonplayerinfo",
                    {"LeagueID": "00", "PlayerID": str(pid)},
                )
                df = to_df(p, name="CommonPlayerInfo")
                if df.empty:
                    return df
                df = utils.normalize_columns(df)
                df["season"] = season
                return df
            except Exception as e:
                log.warning("Skipping player_info player_id=%s: %s", pid, e)
                return pd.DataFrame()

        player_dfs = await asyncio.gather(*[fetch_player_info(pid) for pid in player_ids])
        player_list = [d for d in player_dfs if not d.empty]
        player_info = pd.concat(player_list, ignore_index=True) if player_list else pd.DataFrame()
        log.info("  player_info: %d rows", len(player_info))

    tables = {
        "team_game_logs": team_logs,
        "player_game_logs": player_logs,
        "team_rosters": rosters,
        "common_all_players": common_players,
        "schedule": schedule,
        "box_summaries": box_summaries,
        "player_info": player_info,
    }
    log.info(
        "Raw fetch complete: team_game_logs=%d, player_game_logs=%d, team_rosters=%d, "
        "common_all_players=%d, schedule=%d, box_summaries=%d, player_info=%d",
        len(team_logs),
        len(player_logs),
        len(rosters),
        len(common_players),
        len(schedule),
        len(box_summaries),
        len(player_info),
    )
    return tables


def load_all_raw(season: str, season_type: str = "Regular Season") -> dict[str, pd.DataFrame]:
    """Fetch all raw tables from NBA stats API (async, 3 concurrent workers).

    Core: team logs, player logs, rosters.
    Dimensions: common_all_players, player_info, schedule, box_summaries.

    Args:
        season (str): Season year (e.g. 2026 for 2025-26).
        season_type (str, optional): NBA API season type. Defaults to "Regular Season".

    Returns:
        dict[str, pd.DataFrame]: Raw table DataFrames.
    """
    semaphore = asyncio.Semaphore(api.CONCURRENT_REQUESTS)
    headers = {**api.STATS_HEADERS}
    headers["Connection"] = "keep-alive"

    async def run() -> dict[str, pd.DataFrame]:
        async with aiohttp.ClientSession(headers=headers) as session:
            return await _load_all_raw_async(session, semaphore, season, season_type)

    return asyncio.run(run())

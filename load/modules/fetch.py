"""Fetch raw NBA data from stats.nba.com: game logs, rosters, schedule, and dimensions."""

from __future__ import annotations

import logging

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

    game_ids = (
        team_game_logs["game_id"].dropna().astype(str).str.strip().unique().tolist()
    )
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


def load_all_raw(season: str, season_type: str = "Regular Season") -> dict[str, pd.DataFrame]:
    """Fetch all raw tables from NBA stats API.

    Core: team logs, player logs, rosters.
    Dimensions: common_all_players, player_info, schedule, box_summaries.

    Args:
        season (str): Season year (e.g. 2026 for 2025-26).
        season_type (str, optional): NBA API season type. Defaults to "Regular Season".

    Returns:
        dict[str, pd.DataFrame]: Raw table DataFrames.
    """
    log.info("Fetching raw NBA stats data for season=%s season_type=%s", season, season_type)
    team_logs = load_team_game_logs(season=season, season_type=season_type)
    player_logs = load_player_game_logs(season=season, season_type=season_type)
    rosters = load_team_rosters(season=season, team_game_logs=team_logs)

    common_players = load_common_all_players(season=season)
    schedule = load_schedule(
        team_game_logs=team_logs, season=season, season_type=season_type
    )
    box_summaries = load_box_score_summaries(
        team_game_logs=team_logs, season=season, season_type=season_type
    )
    player_info = load_player_info(common_all_players=common_players, season=season)

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

"""Fetch raw NBA data from stats.nba.com: game logs and rosters."""

from __future__ import annotations

import logging

import pandas as pd

from load.modules import api, utils

log = logging.getLogger(__name__)


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


def load_all_raw(season: str, season_type: str = "Regular Season") -> dict[str, pd.DataFrame]:
    """Fetch all raw tables (team logs, player logs, rosters) from NBA stats API.

    Args:
        season (str): Season year (e.g. 2026 for 2025-26).
        season_type (str, optional): NBA API season type. Defaults to "Regular Season".

    Returns:
        dict[str, pd.DataFrame]: Keys team_game_logs, player_game_logs, team_rosters.
    """
    log.info("Fetching raw NBA stats data for season=%s season_type=%s", season, season_type)
    team_logs = load_team_game_logs(season=season, season_type=season_type)
    player_logs = load_player_game_logs(season=season, season_type=season_type)
    rosters = load_team_rosters(season=season, team_game_logs=team_logs)

    tables = {
        "team_game_logs": team_logs,
        "player_game_logs": player_logs,
        "team_rosters": rosters,
    }
    log.info(
        "Raw fetch complete: team_game_logs=%d, player_game_logs=%d, team_rosters=%d",
        len(team_logs),
        len(player_logs),
        len(rosters),
    )
    return tables

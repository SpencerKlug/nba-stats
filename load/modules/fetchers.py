"""Async fetchers: small, focused functions that each load one table or logical group.

Each fetcher receives:
  - one: (endpoint, params) -> Awaitable[dict]
  - to_df: (payload, name?, index?) -> DataFrame
  - ctx: FetchContext
  - Optional DataFrames from earlier fetchers (dependencies)
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Callable, Awaitable

import pandas as pd

from load.models import (
    BoxScoreParams,
    CommonAllPlayersParams,
    CommonPlayoffSeriesParams,
    CommonPlayerInfoParams,
    CommonTeamRosterParams,
    CommonTeamYearsParams,
    DraftHistoryParams,
    Endpoint,
    LeagueDashLineupsParams,
    LeagueGameLogParams,
    PlayByPlayParams,
    ResultSet,
    ScoreboardParams,
    ShotChartParams,
    TeamDashLineupsParams,
)
from load.modules import utils

log = logging.getLogger(__name__)

OneFn = Callable[[str, dict[str, str]], Awaitable[dict]]
ToDfFn = Callable[..., pd.DataFrame]


def _game_date_for_api(d: str) -> str:
    try:
        dt = pd.to_datetime(d)
        return dt.strftime("%m/%d/%Y")
    except Exception:
        return str(d)


@dataclass
class FetchContext:
    season: str
    season_label: str
    season_type: str
    limit: int | None
    skip_lineups: bool


async def fetch_core(
    one: OneFn, to_df: ToDfFn, ctx: FetchContext
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Team logs, player logs, common_all_players."""
    log.info("Fetching team logs, player logs, commonallplayers (3 parallel)")
    team_params = LeagueGameLogParams(season=ctx.season_label, season_type=ctx.season_type, player_or_team="T")
    player_params = LeagueGameLogParams(season=ctx.season_label, season_type=ctx.season_type, player_or_team="P")
    common_params = CommonAllPlayersParams(season=ctx.season_label)
    team_p, player_p, common_p = await asyncio.gather(
        one(Endpoint.LEAGUE_GAME_LOG.value, team_params.to_api_dict()),
        one(Endpoint.LEAGUE_GAME_LOG.value, player_params.to_api_dict()),
        one(Endpoint.COMMON_ALL_PLAYERS.value, common_params.to_api_dict()),
    )
    team_logs = to_df(team_p)
    if not team_logs.empty:
        team_logs = utils.normalize_columns(team_logs)
        team_logs["season"] = ctx.season
        team_logs["season_label"] = ctx.season_label
        team_logs["season_type"] = ctx.season_type
    player_logs = to_df(player_p)
    if not player_logs.empty:
        player_logs = utils.normalize_columns(player_logs)
        player_logs["season"] = ctx.season
        player_logs["season_label"] = ctx.season_label
        player_logs["season_type"] = ctx.season_type
    common = to_df(common_p, name=ResultSet.COMMON_ALL_PLAYERS.value)
    if not common.empty:
        common = utils.normalize_columns(common)
        common["season"] = ctx.season
        common["season_label"] = ctx.season_label
    log.info("  team_game_logs=%d, player_game_logs=%d, common_all_players=%d", len(team_logs), len(player_logs), len(common))
    return team_logs, player_logs, common


async def fetch_rosters_schedule(
    one: OneFn, to_df: ToDfFn, ctx: FetchContext, team_logs: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rosters and schedule. Depends on team_logs."""
    if team_logs.empty or "team_id" not in team_logs.columns:
        return pd.DataFrame(), pd.DataFrame()
    teams = team_logs[["team_id", "team_abbreviation"]].drop_duplicates().sort_values("team_abbreviation")
    if ctx.limit is not None:
        teams = teams.head(ctx.limit)
    dates = team_logs["game_date"].dropna().unique().tolist()
    if ctx.limit is not None:
        dates = sorted(dates)[: ctx.limit]
    log.info("Loading rosters for %d teams, schedule for %d dates", len(teams), len(dates))

    async def roster(row: Any) -> pd.DataFrame:
        tid, abbrev = int(row.team_id), str(row.team_abbreviation)
        try:
            params = CommonTeamRosterParams(season=ctx.season_label, team_id=str(tid))
            p = await one(Endpoint.COMMON_TEAM_ROSTER.value, params.to_api_dict())
            df = to_df(p, name=ResultSet.COMMON_TEAM_ROSTER.value)
            if df.empty:
                return df
            df = utils.normalize_columns(df)
            df["team_id"] = tid
            df["team_abbreviation"] = abbrev
            df["season"] = ctx.season
            df["season_label"] = ctx.season_label
            return df
        except Exception as e:
            log.warning("Skipping roster team_id=%s: %s", tid, e)
            return pd.DataFrame()

    async def scoreboard(d: str) -> pd.DataFrame:
        try:
            params = ScoreboardParams(game_date=_game_date_for_api(d))
            p = await one(Endpoint.SCOREBOARD.value, params.to_api_dict())
            df = to_df(p, name=ResultSet.GAME_HEADER.value)
            if df.empty:
                return df
            df = utils.normalize_columns(df)
            df["season"] = ctx.season
            df["season_type"] = ctx.season_type
            return df
        except Exception as e:
            log.warning("Skipping scoreboard date=%s: %s", d, e)
            return pd.DataFrame()

    roster_list = [roster(row) for row in teams.itertuples(index=False)]
    sched_list = [scoreboard(str(d)) for d in sorted(dates)]
    roster_dfs, sched_dfs = await asyncio.gather(
        asyncio.gather(*roster_list),
        asyncio.gather(*sched_list),
    )
    rosters = pd.concat([d for d in roster_dfs if not d.empty], ignore_index=True) if roster_dfs else pd.DataFrame()
    schedule = pd.concat([d for d in sched_dfs if not d.empty], ignore_index=True) if sched_dfs else pd.DataFrame()
    if not schedule.empty:
        schedule = schedule.drop_duplicates(subset=["game_id"])
    log.info("  team_rosters=%d, schedule=%d games", len(rosters), len(schedule))
    return rosters, schedule


async def fetch_reference(
    one: OneFn, to_df: ToDfFn, ctx: FetchContext
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """common_team_years, draft_history, common_playoff_series."""
    log.info("Loading commonteamyears, drafthistory, commonplayoffseries")
    ct_params = CommonTeamYearsParams()
    dh_params = DraftHistoryParams()
    cps_params = CommonPlayoffSeriesParams(season=ctx.season_label)
    a, b, c = await asyncio.gather(
        one(Endpoint.COMMON_TEAM_YEARS.value, ct_params.to_api_dict()),
        one(Endpoint.DRAFT_HISTORY.value, dh_params.to_api_dict()),
        one(Endpoint.COMMON_PLAYOFF_SERIES.value, cps_params.to_api_dict()),
    )
    ct = utils.normalize_columns(to_df(a, index=0))
    if not ct.empty:
        ct["season"] = ctx.season
    dh = utils.normalize_columns(to_df(b, index=0))
    if not dh.empty:
        dh["season"] = ctx.season
    cps = utils.normalize_columns(to_df(c, index=0))
    if not cps.empty:
        cps["season"] = ctx.season
    log.info("  common_team_years=%d, draft_history=%d, common_playoff_series=%d", len(ct), len(dh), len(cps))
    return ct, dh, cps


async def fetch_lineups(
    one: OneFn, to_df: ToDfFn, ctx: FetchContext, team_logs: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """league_dash_lineups, team_dash_lineups."""
    if ctx.skip_lineups:
        log.info("Skipping lineup endpoints")
        return pd.DataFrame(), pd.DataFrame()
    log.info("Loading leaguedashlineups and teamdashlineups")
    league = pd.DataFrame()
    try:
        ldl_params = LeagueDashLineupsParams(season=ctx.season_label, season_type=ctx.season_type)
        p = await one(Endpoint.LEAGUE_DASH_LINEUPS.value, ldl_params.to_api_dict())
        league = to_df(p, index=0)
        if not league.empty:
            league = utils.normalize_columns(league)
            league["season"] = ctx.season
            league["season_type"] = ctx.season_type
    except Exception as e:
        log.warning("leaguedashlineups failed: %s", e)
    team_ids = team_logs["team_id"].drop_duplicates().dropna().astype(int).unique().tolist() if not team_logs.empty and "team_id" in team_logs.columns else []
    if ctx.limit is not None:
        team_ids = team_ids[: ctx.limit]
    team_lineups = pd.DataFrame()
    if team_ids:
        async def tl(tid: int) -> pd.DataFrame:
            try:
                tdl_params = TeamDashLineupsParams(season=ctx.season_label, season_type=ctx.season_type, team_id=str(tid))
                p = await one(Endpoint.TEAM_DASH_LINEUPS.value, tdl_params.to_api_dict())
                df = to_df(p, index=0)
                if df.empty:
                    return df
                df = utils.normalize_columns(df)
                df["team_id"] = tid
                df["season"] = ctx.season
                df["season_type"] = ctx.season_type
                return df
            except Exception as e:
                log.warning("teamdashlineups team_id=%s: %s", tid, e)
                return pd.DataFrame()
        dfs = await asyncio.gather(*[tl(tid) for tid in team_ids])
        team_lineups = pd.concat([d for d in dfs if not d.empty], ignore_index=True) if dfs else pd.DataFrame()
    log.info("  league_dash_lineups=%d, team_dash_lineups=%d", len(league), len(team_lineups))
    return league, team_lineups


async def fetch_box_and_pbp(
    one: OneFn, to_df: ToDfFn, ctx: FetchContext, team_logs: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """box_summaries, box_advanced, box_traditional, playbyplay."""
    if team_logs.empty or "game_id" not in team_logs.columns:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    game_ids = team_logs["game_id"].dropna().astype(str).str.strip().unique().tolist()
    if ctx.limit is not None:
        game_ids = game_ids[: ctx.limit]
    log.info("Loading box scores and play-by-play for %d games", len(game_ids))

    async def box(gid: str):
        try:
            params = BoxScoreParams(game_id=gid)
            p = await one(Endpoint.BOX_SCORE_SUMMARY.value, params.to_api_dict())
            df = to_df(p, name=ResultSet.GAME_SUMMARY.value)
            if df.empty:
                return df
            df = utils.normalize_columns(df)
            df["season"] = ctx.season
            df["season_type"] = ctx.season_type
            return df
        except Exception as e:
            log.warning("box summary game_id=%s: %s", gid, e)
            return pd.DataFrame()

    async def adv(gid: str):
        try:
            params = BoxScoreParams(game_id=gid)
            p = await one(Endpoint.BOX_SCORE_ADVANCED.value, params.to_api_dict())
            df = to_df(p, index=0)
            if df.empty:
                return df
            df = utils.normalize_columns(df)
            df["game_id"] = gid
            df["season"] = ctx.season
            df["season_type"] = ctx.season_type
            return df
        except Exception as e:
            return pd.DataFrame()

    async def trad(gid: str):
        try:
            params = BoxScoreParams(game_id=gid)
            p = await one(Endpoint.BOX_SCORE_TRADITIONAL.value, params.to_api_dict())
            df = to_df(p, index=0)
            if df.empty:
                return df
            df = utils.normalize_columns(df)
            df["game_id"] = gid
            df["season"] = ctx.season
            df["season_type"] = ctx.season_type
            return df
        except Exception as e:
            return pd.DataFrame()

    async def pbp(gid: str):
        try:
            params = PlayByPlayParams(game_id=gid)
            p = await one(Endpoint.PLAY_BY_PLAY.value, params.to_api_dict())
            df = to_df(p, index=0)
            if df.empty:
                return df
            df = utils.normalize_columns(df)
            df["game_id"] = gid
            df["season"] = ctx.season
            df["season_type"] = ctx.season_type
            return df
        except Exception as e:
            return pd.DataFrame()

    box_dfs, adv_dfs, trad_dfs, pbp_dfs = await asyncio.gather(
        asyncio.gather(*[box(gid) for gid in game_ids]),
        asyncio.gather(*[adv(gid) for gid in game_ids]),
        asyncio.gather(*[trad(gid) for gid in game_ids]),
        asyncio.gather(*[pbp(gid) for gid in game_ids]),
    )
    box_sum = pd.concat([d for d in box_dfs if not d.empty], ignore_index=True) if box_dfs else pd.DataFrame()
    box_adv = pd.concat([d for d in adv_dfs if not d.empty], ignore_index=True) if adv_dfs else pd.DataFrame()
    box_trad = pd.concat([d for d in trad_dfs if not d.empty], ignore_index=True) if trad_dfs else pd.DataFrame()
    pbp_df = pd.concat([d for d in pbp_dfs if not d.empty], ignore_index=True) if pbp_dfs else pd.DataFrame()
    log.info("  box_summaries=%d, box_advanced=%d, box_traditional=%d, playbyplay=%d", len(box_sum), len(box_adv), len(box_trad), len(pbp_df))
    return box_sum, box_adv, box_trad, pbp_df


async def fetch_shot_charts(
    one: OneFn, to_df: ToDfFn, ctx: FetchContext, box_summaries: pd.DataFrame
) -> pd.DataFrame:
    """Shot charts. Depends on box_summaries for game_id + home/visitor team_ids."""
    if box_summaries.empty or "home_team_id" not in box_summaries.columns:
        return pd.DataFrame()
    rows = box_summaries[["game_id", "home_team_id", "visitor_team_id"]].drop_duplicates()
    tasks = []
    for _, row in rows.iterrows():
        for tid in (int(row["home_team_id"]), int(row["visitor_team_id"])):
            tasks.append((str(row["game_id"]), tid))
    log.info("Loading shot charts for %d game-team pairs", len(tasks))

    async def fetch(gid: str, tid: int) -> pd.DataFrame:
        try:
            params = ShotChartParams(season=ctx.season_label, season_type=ctx.season_type, game_id=gid, team_id=str(tid))
            p = await one(Endpoint.SHOT_CHART.value, params.to_api_dict())
            df = to_df(p, index=0)
            if df.empty:
                return df
            df = utils.normalize_columns(df)
            df["game_id"] = gid
            df["team_id"] = tid
            df["season"] = ctx.season
            df["season_type"] = ctx.season_type
            return df
        except Exception as e:
            return pd.DataFrame()

    dfs = await asyncio.gather(*[fetch(gid, tid) for gid, tid in tasks])
    out = pd.concat([d for d in dfs if not d.empty], ignore_index=True) if dfs else pd.DataFrame()
    log.info("  shot_charts=%d", len(out))
    return out


async def fetch_player_info(
    one: OneFn, to_df: ToDfFn, ctx: FetchContext, common_players: pd.DataFrame
) -> pd.DataFrame:
    """Player info (bio). Depends on common_all_players."""
    if common_players.empty or "person_id" not in common_players.columns:
        return pd.DataFrame()
    pids = common_players["person_id"].dropna().unique().tolist()
    if ctx.limit is not None:
        pids = pids[: ctx.limit]
    log.info("Loading player info for %d players", len(pids))

    async def fetch(pid: Any) -> pd.DataFrame:
        try:
            params = CommonPlayerInfoParams(player_id=str(pid))
            p = await one(Endpoint.COMMON_PLAYER_INFO.value, params.to_api_dict())
            df = to_df(p, name=ResultSet.COMMON_PLAYER_INFO.value)
            if df.empty:
                return df
            df = utils.normalize_columns(df)
            df["season"] = ctx.season
            return df
        except Exception as e:
            return pd.DataFrame()

    dfs = await asyncio.gather(*[fetch(pid) for pid in pids])
    out = pd.concat([d for d in dfs if not d.empty], ignore_index=True) if dfs else pd.DataFrame()
    log.info("  player_info=%d", len(out))
    return out

"""NBA stats.nba.com endpoint definitions: paths, result sets, and request params.

All params serialize to dict[str, str] for the API. Use .to_api_dict() on each model.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


# --- Endpoint paths ---


class Endpoint(str, Enum):
    LEAGUE_GAME_LOG = "leaguegamelog"
    COMMON_ALL_PLAYERS = "commonallplayers"
    COMMON_TEAM_ROSTER = "commonteamroster"
    SCOREBOARD = "scoreboardv2"
    COMMON_TEAM_YEARS = "commonteamyears"
    DRAFT_HISTORY = "drafthistory"
    COMMON_PLAYOFF_SERIES = "commonplayoffseries"
    LEAGUE_DASH_LINEUPS = "leaguedashlineups"
    TEAM_DASH_LINEUPS = "teamdashlineups"
    BOX_SCORE_SUMMARY = "boxscoresummaryv2"
    BOX_SCORE_ADVANCED = "boxscoreadvancedv2"
    BOX_SCORE_TRADITIONAL = "boxscoretraditionalv2"
    PLAY_BY_PLAY = "playbyplayv2"
    SHOT_CHART = "shotchartdetail"
    COMMON_PLAYER_INFO = "commonplayerinfo"


# --- Result set names (for parsing API response) ---


class ResultSet(str, Enum):
    COMMON_ALL_PLAYERS = "CommonAllPlayers"
    COMMON_TEAM_ROSTER = "CommonTeamRoster"
    GAME_HEADER = "GameHeader"
    GAME_SUMMARY = "GameSummary"
    COMMON_PLAYER_INFO = "CommonPlayerInfo"
    # Index 0 used (no named result set): commonteamyears, drafthistory, commonplayoffseries,
    # leaguedashlineups, teamdashlineups, box advanced/traditional, playbyplay, shotchart


# --- Common constants ---


LEAGUE_ID_NBA = "00"


# --- Base for API params (all values become strings) ---


# --- Param models ---


class LeagueGameLogParams(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    counter: str = "1000"
    direction: str = "DESC"
    league_id: str = Field(default=LEAGUE_ID_NBA, alias="LeagueID")
    player_or_team: str = Field(..., alias="PlayerOrTeam")  # "T" or "P"
    season: str = Field(..., alias="Season")
    season_type: str = Field(..., alias="SeasonType")
    sorter: str = Field(default="DATE", alias="Sorter")

    def to_api_dict(self) -> dict[str, str]:
        return {
            "Counter": self.counter,
            "Direction": self.direction,
            "LeagueID": self.league_id,
            "PlayerOrTeam": self.player_or_team,
            "Season": self.season,
            "SeasonType": self.season_type,
            "Sorter": self.sorter,
        }


class CommonAllPlayersParams(BaseModel):
    league_id: str = Field(default=LEAGUE_ID_NBA, alias="LeagueID")
    season: str = Field(..., alias="Season")
    is_only_current_season: str = Field(default="1", alias="IsOnlyCurrentSeason")

    def to_api_dict(self) -> dict[str, str]:
        return {
            "LeagueID": self.league_id,
            "Season": self.season,
            "IsOnlyCurrentSeason": self.is_only_current_season,
        }


class CommonTeamRosterParams(BaseModel):
    league_id: str = Field(default=LEAGUE_ID_NBA, alias="LeagueID")
    season: str = Field(..., alias="Season")
    team_id: str = Field(..., alias="TeamID")

    def to_api_dict(self) -> dict[str, str]:
        return {"LeagueID": self.league_id, "Season": self.season, "TeamID": self.team_id}


class ScoreboardParams(BaseModel):
    league_id: str = Field(default=LEAGUE_ID_NBA, alias="LeagueID")
    game_date: str = Field(..., alias="GameDate")
    day_offset: str = Field(default="0", alias="DayOffset")

    def to_api_dict(self) -> dict[str, str]:
        return {"LeagueID": self.league_id, "GameDate": self.game_date, "DayOffset": self.day_offset}


class CommonTeamYearsParams(BaseModel):
    league_id: str = Field(default=LEAGUE_ID_NBA, alias="LeagueID")

    def to_api_dict(self) -> dict[str, str]:
        return {"LeagueID": self.league_id}


class DraftHistoryParams(BaseModel):
    league_id: str = Field(default=LEAGUE_ID_NBA, alias="LeagueID")

    def to_api_dict(self) -> dict[str, str]:
        return {"LeagueID": self.league_id}


class CommonPlayoffSeriesParams(BaseModel):
    league_id: str = Field(default=LEAGUE_ID_NBA, alias="LeagueID")
    season: str = Field(..., alias="Season")

    def to_api_dict(self) -> dict[str, str]:
        return {"LeagueID": self.league_id, "Season": self.season}


class LeagueDashLineupsParams(BaseModel):
    league_id: str = Field(default=LEAGUE_ID_NBA, alias="LeagueID")
    season: str = Field(..., alias="Season")
    season_type: str = Field(..., alias="SeasonType")
    group_quantity: str = Field(default="5", alias="GroupQuantity")
    per_mode: str = Field(default="Totals", alias="PerMode")
    measure_type: str = Field(default="Base", alias="MeasureType")

    def to_api_dict(self) -> dict[str, str]:
        return {
            "LeagueID": self.league_id,
            "Season": self.season,
            "SeasonType": self.season_type,
            "GroupQuantity": self.group_quantity,
            "PerMode": self.per_mode,
            "MeasureType": self.measure_type,
        }


class TeamDashLineupsParams(BaseModel):
    league_id: str = Field(default=LEAGUE_ID_NBA, alias="LeagueID")
    season: str = Field(..., alias="Season")
    season_type: str = Field(..., alias="SeasonType")
    team_id: str = Field(..., alias="TeamID")
    group_quantity: str = Field(default="5", alias="GroupQuantity")

    def to_api_dict(self) -> dict[str, str]:
        return {
            "LeagueID": self.league_id,
            "Season": self.season,
            "SeasonType": self.season_type,
            "TeamID": self.team_id,
            "GroupQuantity": self.group_quantity,
        }


class BoxScoreParams(BaseModel):
    game_id: str = Field(..., alias="GameID")
    start_period: str = Field(default="0", alias="StartPeriod")
    end_period: str = Field(default="14", alias="EndPeriod")
    start_range: str = Field(default="0", alias="StartRange")
    end_range: str = Field(default="2147483647", alias="EndRange")
    range_type: str = Field(default="0", alias="RangeType")

    def to_api_dict(self) -> dict[str, str]:
        return {
            "GameID": self.game_id,
            "StartPeriod": self.start_period,
            "EndPeriod": self.end_period,
            "StartRange": self.start_range,
            "EndRange": self.end_range,
            "RangeType": self.range_type,
        }


class PlayByPlayParams(BaseModel):
    game_id: str = Field(..., alias="GameID")
    start_period: str = Field(default="0", alias="StartPeriod")
    end_period: str = Field(default="14", alias="EndPeriod")

    def to_api_dict(self) -> dict[str, str]:
        return {"GameID": self.game_id, "StartPeriod": self.start_period, "EndPeriod": self.end_period}


class ShotChartParams(BaseModel):
    league_id: str = Field(default=LEAGUE_ID_NBA, alias="LeagueID")
    season: str = Field(..., alias="Season")
    season_type: str = Field(..., alias="SeasonType")
    game_id: str = Field(..., alias="GameID")
    team_id: str = Field(..., alias="TeamID")

    def to_api_dict(self) -> dict[str, str]:
        return {
            "LeagueID": self.league_id,
            "Season": self.season,
            "SeasonType": self.season_type,
            "GameID": self.game_id,
            "TeamID": self.team_id,
        }


class CommonPlayerInfoParams(BaseModel):
    league_id: str = Field(default=LEAGUE_ID_NBA, alias="LeagueID")
    player_id: str = Field(..., alias="PlayerID")

    def to_api_dict(self) -> dict[str, str]:
        return {"LeagueID": self.league_id, "PlayerID": self.player_id}

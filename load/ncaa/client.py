"""NCAA stats.ncaa.org client: facade re-exporting all page modules.

Men's basketball: sport_code=MBB, division=1. Academic year is the year
the season ends (e.g. 2026 for 2025-26). Pages are HTML with tables;
no public JSON API.

Page-specific logic lives in:
  - core: session, HTTP, table/link helpers, constants
  - team_list: /team/inst_team_list
  - team_season: /team/index (schedule)
  - roster: /team/roster
  - scoreboard: /contests/scoreboards
  - box_score: /contests/{id}/box_score
"""

from __future__ import annotations

from load.ncaa import box_score, core, roster, scoreboard, team_list, team_season

# Constants
NCAA_BASE = core.NCAA_BASE
NCAA_HEADERS = core.NCAA_HEADERS
SPORT_CODE_MBB = core.SPORT_CODE_MBB
DIVISION_I = core.DIVISION_I
REQUEST_DELAY_SECONDS = core.REQUEST_DELAY_SECONDS

# Team list
get_team_list_page = team_list.get_team_list_page
parse_team_list_html = team_list.parse_team_list_html

# Team season / schedule
get_team_season_page = team_season.get_team_season_page
get_team_schedule_page = team_season.get_team_schedule_page
parse_schedule_contest_ids = team_season.parse_schedule_contest_ids

# Roster
get_team_roster_page = roster.get_team_roster_page

# Scoreboard
get_scoreboard_page = scoreboard.get_scoreboard_page
parse_scoreboard_to_games = scoreboard.parse_scoreboard_to_games

# Contest IDs (used by scoreboard and schedule)
parse_contest_ids_from_html = core.parse_contest_ids_from_html

# Box score
get_box_score_page = box_score.get_box_score_page
parse_box_score_game_info = box_score.parse_box_score_game_info
parse_box_score_player_stats = box_score.parse_box_score_player_stats

# Generic table helper
html_table_to_df = core.html_table_to_df

__all__ = [
    "NCAA_BASE",
    "NCAA_HEADERS",
    "SPORT_CODE_MBB",
    "DIVISION_I",
    "REQUEST_DELAY_SECONDS",
    "get_team_list_page",
    "parse_team_list_html",
    "get_team_season_page",
    "get_team_schedule_page",
    "parse_schedule_contest_ids",
    "get_team_roster_page",
    "get_scoreboard_page",
    "parse_scoreboard_to_games",
    "parse_contest_ids_from_html",
    "get_box_score_page",
    "parse_box_score_game_info",
    "parse_box_score_player_stats",
    "html_table_to_df",
]

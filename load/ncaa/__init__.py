"""NCAA men's basketball data ingestion from stats.ncaa.org."""

from load.ncaa.box_score import load_player_box_scores_and_schedule
from load.ncaa.client import client
from load.ncaa.season import load_ncaa_mbb_season
from load.ncaa.scoreboard import load_game_list
from load.ncaa.team_list import load_team_list

__all__ = [
    "client",
    "load_game_list",
    "load_ncaa_mbb_season",
    "load_player_box_scores_and_schedule",
    "load_team_list",
]

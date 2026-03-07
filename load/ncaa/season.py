"""NCAA season orchestration: load one season of MBB data into raw tables."""

from __future__ import annotations

import pandas as pd

from load.ncaa import box_score, scoreboard, team_list
from load.ncaa.core import DIVISION_I


def load_ncaa_mbb_season(
    season: str,
    division: str = DIVISION_I,
    include_team_list: bool = True,
    include_games: bool = True,
    include_box_scores: bool = True,
    use_team_schedules: bool = False,
    limit: int | None = None,
) -> dict[str, pd.DataFrame]:
    """Load one season of NCAA men's basketball data into raw tables."""
    result: dict[str, pd.DataFrame] = {}

    if include_team_list:
        teams = team_list.load_team_list(season=season, division=division)
        if not teams.empty:
            result["ncaa_team_list"] = teams

    if include_games or include_box_scores:
        games_df, contest_ids = scoreboard.load_game_list(
            season=season,
            division=division,
            use_team_schedules=use_team_schedules,
            limit=limit,
        )
        if contest_ids:
            if include_box_scores:
                box_df, schedule_from_box = (
                    box_score.load_player_box_scores_and_schedule(
                        contest_ids, season, limit=limit
                    )
                )
                if not box_df.empty:
                    result["ncaa_player_box_scores"] = box_df
                if include_games and not schedule_from_box.empty:
                    result["ncaa_schedule"] = schedule_from_box
            elif include_games and not games_df.empty:
                result["ncaa_schedule"] = games_df

    return result

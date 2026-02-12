#!/usr/bin/env python3
"""
Example: fetch NBA data from Basketball-Reference and optionally save to CSV.
"""

from basketball_reference import (
    get_page,
    standings,
    season_leaders,
    team_roster,
    player_stats_per_game,
    schedule_results,
)


def main() -> None:
    season = "2026"  # 2025-26 season per Basketball-Reference

    print("Fetching 2025-26 standings...")
    df_standings = standings(season=season)
    print(df_standings.head(10).to_string())
    df_standings.to_csv("standings_2025-26.csv", index=False)
    print("Saved standings_2025-26.csv\n")

    print("Fetching per-game leaders (points)...")
    df_leaders = season_leaders(season=season, stat="PTS")
    print(df_leaders.head(5).to_string())
    df_leaders.to_csv("leaders_per_game.csv", index=False)
    print("Saved leaders_per_game.csv\n")

    print("Fetching Celtics roster...")
    df_roster = team_roster("BOS", season=season)
    print(df_roster.head(10).to_string())
    df_roster.to_csv("celtics_roster.csv", index=False)
    print("Saved celtics_roster.csv\n")

    # Uncomment to fetch more (adds delay):
    # print("Fetching full per-game stats...")
    # df_stats = player_stats_per_game(season=season)
    # df_stats.to_csv("player_per_game.csv", index=False)
    # print("Fetching schedule...")
    # df_schedule = schedule_results(season=season)
    # df_schedule.to_csv("schedule.csv", index=False)


if __name__ == "__main__":
    main()

-- Team game logs from stats.nba.com (one row per team per game)
select
    game_id,
    game_date,
    season,
    season_label,
    season_type,
    team_id,
    team_abbreviation as team,
    team_name,
    matchup,
    wl,
    min,
    pts,
    reb,
    ast,
    stl,
    blk,
    tov,
    plus_minus
from {{ source("raw", "team_game_logs") }}
where game_id is not null

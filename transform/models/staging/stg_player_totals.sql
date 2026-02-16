-- Player game logs from stats.nba.com (raw, one row per player per game)
select
    game_id,
    game_date,
    season,
    season_label,
    season_type,
    player_id,
    player_name as player,
    team_id,
    team_abbreviation as team,
    team_name,
    min,
    pts,
    reb,
    ast,
    stl,
    blk,
    tov,
    fgm,
    fga,
    fg_pct,
    fg3m,
    fg3a,
    fg3_pct,
    ftm,
    fta,
    ft_pct,
    oreb,
    dreb,
    pf
from {{ source("raw", "player_game_logs") }}
where game_id is not null
  and player_id is not null

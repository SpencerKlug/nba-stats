-- Game dimension: one row per game with arena, teams, status
select
    game_id,
    game_date,
    game_status_id,
    game_status_text,
    home_team_id,
    visitor_team_id,
    arena_name,
    season,
    season_type
from {{ ref("int_game_schedule") }}
order by game_date desc, game_id

-- Game summary from boxscoresummaryv2 GameSummary
select
    game_id,
    game_date_est as game_date,
    game_status_id,
    game_status_text,
    home_team_id,
    visitor_team_id,
    coalesce(arena_name, '') as arena_name,
    season,
    season_type
from {{ source("raw", "box_summaries") }}
where game_id is not null

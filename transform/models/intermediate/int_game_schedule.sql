-- Game dimension: merge schedule (scoreboard) with box summaries
-- Prefer box_summaries for arena/game metadata; fall back to schedule
with sched as (
    select
        game_id,
        game_date,
        game_status_id,
        game_status_text,
        home_team_id,
        visitor_team_id,
        arena_name as arena_name_sched,
        season,
        season_type
    from {{ ref("stg_schedule") }}
),
box as (
    select
        game_id,
        game_date,
        game_status_id,
        game_status_text,
        home_team_id,
        visitor_team_id,
        arena_name as arena_name_box,
        season,
        season_type
    from {{ ref("stg_box_summaries") }}
)
select
    coalesce(b.game_id, s.game_id) as game_id,
    coalesce(b.game_date, s.game_date) as game_date,
    coalesce(b.game_status_id, s.game_status_id) as game_status_id,
    coalesce(b.game_status_text, s.game_status_text) as game_status_text,
    coalesce(b.home_team_id, s.home_team_id) as home_team_id,
    coalesce(b.visitor_team_id, s.visitor_team_id) as visitor_team_id,
    coalesce(b.arena_name_box, s.arena_name_sched, '') as arena_name,
    coalesce(b.season, s.season) as season,
    coalesce(b.season_type, s.season_type) as season_type
from sched s
full outer join box b on s.game_id = b.game_id
where coalesce(b.game_id, s.game_id) is not null
order by game_date desc, game_id

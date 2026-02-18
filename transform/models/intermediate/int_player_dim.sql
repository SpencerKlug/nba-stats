-- Player dimension: merge roster snapshot (stg_players) with bio (stg_player_info)
-- One row per player per season; latest player_info wins on overlap
with players as (
    select
        player_id,
        player_name,
        player_name_sort,
        team_id,
        team_abbrev,
        team_city,
        team_name,
        from_year,
        to_year,
        rosterstatus,
        season,
        season_label
    from {{ ref("stg_players") }}
),
info as (
    select
        player_id,
        first_name,
        last_name,
        birthdate,
        college,
        country,
        height,
        weight,
        position,
        jersey,
        draft_year,
        draft_round,
        draft_pick,
        season as info_season
    from {{ ref("stg_player_info") }}
)
select
    p.player_id,
    p.player_name,
    p.player_name_sort,
    p.team_id,
    p.team_abbrev,
    p.team_city,
    p.team_name,
    p.from_year,
    p.to_year,
    p.rosterstatus,
    p.season,
    p.season_label,
    i.first_name,
    i.last_name,
    i.birthdate,
    i.college,
    i.country,
    i.height,
    i.weight,
    i.position,
    i.jersey,
    i.draft_year,
    i.draft_round,
    i.draft_pick
from players p
left join info i
    on p.player_id = i.player_id
    and p.season = i.info_season
order by p.season desc, p.player_name_sort

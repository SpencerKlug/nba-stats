-- Player dimension: one row per player per season with bio attributes
select
    player_id,
    player_name,
    player_name_sort,
    coalesce(team_abbrev, '') as team,
    team_city,
    team_name,
    from_year,
    to_year,
    rosterstatus,
    season,
    season_label,
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
    draft_pick
from {{ ref("int_player_dim") }}
order by season desc, player_name_sort

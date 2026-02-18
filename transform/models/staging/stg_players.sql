-- Master player list from commonallplayers (one row per player per season)
select
    person_id as player_id,
    display_first_last as player_name,
    display_last_comma_first as player_name_sort,
    coalesce(team_id::varchar, '') as team_id,
    coalesce(team_abbreviation, '') as team_abbrev,
    team_city,
    team_name,
    from_year,
    to_year,
    rosterstatus,
    season,
    season_label
from {{ source("raw", "common_all_players") }}
where person_id is not null

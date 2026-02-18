-- Player bio from commonplayerinfo (height, weight, school, draft)
select
    person_id as player_id,
    display_first_last as player_name,
    first_name,
    last_name,
    birthdate,
    school as college,
    country,
    height,
    weight,
    position,
    jersey,
    coalesce(draft_year::varchar, '') as draft_year,
    coalesce(draft_round::varchar, '') as draft_round,
    coalesce(draft_number::varchar, '') as draft_pick,
    season
from {{ source("raw", "player_info") }}
where person_id is not null

-- Team roster rows from commonteamroster
select
    num as jersey_no,
    player,
    position as pos,
    height,
    weight,
    birth_date,
    exp as experience,
    school as college,
    team_id,
    team_abbreviation as team_abbrev,
    season,
    season_label
from {{ source("raw", "team_rosters") }}
where player is not null

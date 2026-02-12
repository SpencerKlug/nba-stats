-- Roster: one row per player-team-season.
select
    coalesce(no_, no) as jersey_no,
    player,
    pos,
    ht as height,
    wt as weight,
    birth_date,
    birth_exp as birth_country,
    exp as experience,
    college,
    team_abbrev,
    season
from {{ source("raw", "roster") }}
where player is not null

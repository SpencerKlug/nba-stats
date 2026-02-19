-- Team dimension: one row per team with id, abbreviation, name
select
    team_id,
    coalesce(team_abbreviation, '') as team_abbreviation,
    coalesce(team_name, '') as team_name
from {{ ref("int_teams") }}
order by team_abbreviation

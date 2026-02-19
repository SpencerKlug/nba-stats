-- One row per team from team game logs
select
    team_id,
    any_value(team_abbreviation) as team_abbreviation,
    any_value(team_name) as team_name
from {{ source("raw", "team_game_logs") }}
where team_id is not null
group by team_id
order by team_abbreviation

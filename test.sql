select player_id, player
from raw.team_rosters
where school ilike '%Gonzaga%'
group by player_id, player
;
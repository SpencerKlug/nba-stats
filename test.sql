select player_id, player
from raw.team_rosters
where school ilike '%Gonzaga%'
group by player_id, player
;
select *
from raw.team_game_logs;

select *
from main_intermediate.int_game_schedule;

select 
  gs.game_id,
  gs.game_date,arena_name,
  gs.home_team_id,
  t.team_name as home_team_name,
  t2.team_name as visitor_team_name,
  gs.visitor_team_id
from main_intermediate.int_game_schedule as gs
left join main_intermediate.int_teams as t
  on gs.home_team_id = t.team_id
left join main_intermediate.int_teams as t2
  on gs.visitor_team_id = t2.team_id
;

select *
from raw.
where player_name like '%Scoot%'
;
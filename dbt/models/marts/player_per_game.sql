-- Per-game stats derived from raw season totals (no pre-aggregated per-game source).
-- PPG = PTS / G, RPG = TRB / G, etc.
select
    player,
    team,
    pos,
    age,
    season,
    games as g,
    games_started as gs,
    round(minutes_played::double / nullif(games, 0), 1) as mp,
    round(fg::double / nullif(games, 0), 1) as fg,
    round(fga::double / nullif(games, 0), 1) as fga,
    round(fg_pct, 3) as fg_pct,
    round(three_p::double / nullif(games, 0), 1) as "3p",
    round(three_pa::double / nullif(games, 0), 1) as "3pa",
    round(three_p_pct, 3) as "3p_pct",
    round(two_p::double / nullif(games, 0), 1) as "2p",
    round(two_pa::double / nullif(games, 0), 1) as "2pa",
    round(two_p_pct, 3) as "2p_pct",
    round(ft::double / nullif(games, 0), 1) as ft,
    round(fta::double / nullif(games, 0), 1) as fta,
    round(ft_pct, 3) as ft_pct,
    round(off_reb::double / nullif(games, 0), 1) as orb,
    round(def_reb::double / nullif(games, 0), 1) as drb,
    round(tot_reb::double / nullif(games, 0), 1) as trb,
    round(ast::double / nullif(games, 0), 1) as ast,
    round(steals::double / nullif(games, 0), 1) as stl,
    round(blocks::double / nullif(games, 0), 1) as blk,
    round(turnovers::double / nullif(games, 0), 1) as tov,
    round(fouls::double / nullif(games, 0), 1) as pf,
    round(points::double / nullif(games, 0), 1) as pts
from {{ ref("stg_player_totals") }}
where games > 0
order by season desc, pts desc

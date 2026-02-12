-- Raw player season totals (counting stats). Per-game and rates built in marts.
-- Normalized columns: FG% -> fg_1, 3P% -> 3p_1, 2P% -> 2p_1, FT% -> ft_1; eFG% -> efg.
select
    rk,
    player,
    age,
    team,
    pos,
    g as games,
    gs as games_started,
    mp as minutes_played,
    fg,
    fga,
    fg_1 as fg_pct,
    "3p" as three_p,
    "3pa" as three_pa,
    "3p_1" as three_p_pct,
    "2p" as two_p,
    "2pa" as two_pa,
    "2p_1" as two_p_pct,
    efg as efg_pct,
    ft,
    fta,
    ft_1 as ft_pct,
    orb as off_reb,
    drb as def_reb,
    trb as tot_reb,
    ast,
    stl as steals,
    blk as blocks,
    tov as turnovers,
    pf as fouls,
    pts as points,
    season
from {{ source("raw", "player_season_totals") }}
where player is not null
  and trim(cast(player as varchar)) != ''

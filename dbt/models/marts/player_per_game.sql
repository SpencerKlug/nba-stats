-- Per-game player stats from raw player game logs.
with totals as (
    select
        player_id,
        player,
        team,
        season,
        count(distinct game_id) as g,
        sum(coalesce(pts, 0)) as pts_tot,
        sum(coalesce(reb, 0)) as reb_tot,
        sum(coalesce(ast, 0)) as ast_tot,
        sum(coalesce(stl, 0)) as stl_tot,
        sum(coalesce(blk, 0)) as blk_tot,
        sum(coalesce(tov, 0)) as tov_tot,
        sum(coalesce(min, 0)) as min_tot,
        sum(coalesce(fgm, 0)) as fgm_tot,
        sum(coalesce(fga, 0)) as fga_tot,
        sum(coalesce(fg3m, 0)) as fg3m_tot,
        sum(coalesce(fg3a, 0)) as fg3a_tot,
        sum(coalesce(ftm, 0)) as ftm_tot,
        sum(coalesce(fta, 0)) as fta_tot,
        sum(coalesce(oreb, 0)) as oreb_tot,
        sum(coalesce(dreb, 0)) as dreb_tot,
        sum(coalesce(pf, 0)) as pf_tot
    from {{ ref("stg_player_totals") }}
    group by 1,2,3,4
)
select
    player_id,
    player,
    team,
    season,
    g,
    round(min_tot::double / nullif(g, 0), 1) as mp,
    round(pts_tot::double / nullif(g, 0), 1) as pts,
    round(reb_tot::double / nullif(g, 0), 1) as trb,
    round(ast_tot::double / nullif(g, 0), 1) as ast,
    round(stl_tot::double / nullif(g, 0), 1) as stl,
    round(blk_tot::double / nullif(g, 0), 1) as blk,
    round(tov_tot::double / nullif(g, 0), 1) as tov,
    round(fgm_tot::double / nullif(g, 0), 1) as fg,
    round(fga_tot::double / nullif(g, 0), 1) as fga,
    round(fgm_tot::double / nullif(fga_tot, 0), 3) as fg_pct,
    round(fg3m_tot::double / nullif(g, 0), 1) as "3p",
    round(fg3a_tot::double / nullif(g, 0), 1) as "3pa",
    round(fg3m_tot::double / nullif(fg3a_tot, 0), 3) as "3p_pct",
    round(ftm_tot::double / nullif(g, 0), 1) as ft,
    round(fta_tot::double / nullif(g, 0), 1) as fta,
    round(ftm_tot::double / nullif(fta_tot, 0), 3) as ft_pct,
    round(oreb_tot::double / nullif(g, 0), 1) as orb,
    round(dreb_tot::double / nullif(g, 0), 1) as drb,
    round(pf_tot::double / nullif(g, 0), 1) as pf
from totals
where g > 0
order by season desc, pts desc

-- Standings from raw team game logs (no pre-aggregated standings source).
with wins_losses as (
    select
        team,
        count(*) filter (where upper(wl) = 'W') as wins,
        count(*) filter (where upper(wl) = 'L') as losses
    from {{ ref("stg_games") }}
    where wl in ('W', 'L')
    group by team
),

with_pct as (
    select
        team,
        wins,
        losses,
        wins + losses as games_played,
        round(1.0 * wins / nullif(wins + losses, 0), 3) as w_l_pct
    from wins_losses
),

conference as (
    select * from (
        values
            ('ATL', 'East'), ('BOS', 'East'), ('BRK', 'East'), ('CHO', 'East'), ('CHI', 'East'),
            ('CLE', 'East'), ('DET', 'East'), ('IND', 'East'), ('MIA', 'East'), ('MIL', 'East'),
            ('NYK', 'East'), ('ORL', 'East'), ('PHI', 'East'), ('TOR', 'East'), ('WAS', 'East'),
            ('DAL', 'West'), ('DEN', 'West'), ('GSW', 'West'), ('HOU', 'West'), ('LAC', 'West'),
            ('LAL', 'West'), ('MEM', 'West'), ('MIN', 'West'), ('NOP', 'West'), ('OKC', 'West'),
            ('PHO', 'West'), ('POR', 'West'), ('SAC', 'West'), ('SAS', 'West'), ('UTA', 'West')
    ) as t(team, conference)
),

ranked as (
    select
        w.team,
        w.wins,
        w.losses,
        w.w_l_pct,
        c.conference,
        row_number() over (partition by c.conference order by w.wins desc, w.losses asc) as conf_rank
    from with_pct w
    join conference c using (team)
),

leader as (
    select conference, wins as leader_wins, losses as leader_losses
    from ranked
    where conf_rank = 1
)

select
    r.team,
    r.conference,
    r.conf_rank,
    r.wins,
    r.losses,
    r.w_l_pct,
    round((l.leader_wins - r.wins + r.losses - l.leader_losses)::double / 2.0, 1) as gb
from ranked r
join leader l using (conference)
order by r.conference, r.conf_rank

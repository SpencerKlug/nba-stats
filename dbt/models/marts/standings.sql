-- Conference standings derived from raw game results (no pre-aggregated standings).
-- Wins = games won; losses = games lost; W/L% and GB computed here.
-- Raw games use full team names (e.g. "Boston Celtics"); map to abbrev for conference/rank.
with team_abbrev as (
    select * from (
        values
            ('Atlanta Hawks', 'ATL'), ('Boston Celtics', 'BOS'), ('Brooklyn Nets', 'BRK'),
            ('Charlotte Hornets', 'CHO'), ('Chicago Bulls', 'CHI'), ('Cleveland Cavaliers', 'CLE'),
            ('Dallas Mavericks', 'DAL'), ('Denver Nuggets', 'DEN'), ('Detroit Pistons', 'DET'),
            ('Golden State Warriors', 'GSW'), ('Houston Rockets', 'HOU'), ('Indiana Pacers', 'IND'),
            ('Los Angeles Clippers', 'LAC'), ('Los Angeles Lakers', 'LAL'), ('Memphis Grizzlies', 'MEM'),
            ('Miami Heat', 'MIA'), ('Milwaukee Bucks', 'MIL'), ('Minnesota Timberwolves', 'MIN'),
            ('New Orleans Pelicans', 'NOP'), ('New York Knicks', 'NYK'), ('Oklahoma City Thunder', 'OKC'),
            ('Orlando Magic', 'ORL'), ('Philadelphia 76ers', 'PHI'), ('Phoenix Suns', 'PHO'),
            ('Portland Trail Blazers', 'POR'), ('Sacramento Kings', 'SAC'), ('San Antonio Spurs', 'SAS'),
            ('Toronto Raptors', 'TOR'), ('Utah Jazz', 'UTA'), ('Washington Wizards', 'WAS')
    ) as t(team_name, team_abbrev)
),

game_results as (
    select
        g.date,
        a.team_abbrev as away_team,
        h.team_abbrev as home_team,
        g.visitor_pts,
        g.home_pts,
        case when g.visitor_pts > g.home_pts then a.team_abbrev else h.team_abbrev end as winner,
        case when g.visitor_pts < g.home_pts then a.team_abbrev else h.team_abbrev end as loser
    from {{ ref("stg_games") }} g
    left join team_abbrev a on trim(cast(g.visitor as varchar)) = a.team_name
    left join team_abbrev h on trim(cast(g.home as varchar)) = h.team_name
    where g.visitor_pts is not null
      and g.home_pts is not null
      and a.team_abbrev is not null
      and h.team_abbrev is not null
),

team_games as (
    select home_team as team, case when home_team = winner then 1 else 0 end as is_win
    from game_results
    union all
    select away_team as team, case when away_team = winner then 1 else 0 end as is_win
    from game_results
),

wins_losses as (
    select
        team,
        sum(is_win)::int as wins,
        count(*)::int - sum(is_win)::int as losses
    from team_games
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

-- Conference: map team abbrev to conference (East/West)
conference as (
    select * from (
        values
            ('ATL', 'East'), ('BOS', 'East'), ('BRK', 'East'), ('CHO', 'East'), ('CHI', 'East'),
            ('CLE', 'East'), ('DET', 'East'), ('IND', 'East'), ('MIA', 'East'), ('MIL', 'East'),
            ('NYK', 'East'), ('ORL', 'East'), ('PHI', 'East'), ('TOR', 'East'), ('WAS', 'East'),
            ('DAL', 'West'), ('DEN', 'West'), ('GSW', 'West'), ('HOU', 'West'), ('LAC', 'West'),
            ('LAL', 'West'), ('MEM', 'West'), ('MIN', 'West'), ('NOP', 'West'), ('OKC', 'West'),
            ('PHO', 'West'), ('POR', 'West'), ('SAC', 'West'), ('SAS', 'West'), ('UTA', 'West')
    ) as t(team_abbrev, conference)
),

-- Games behind: 0 for leader, then (leader_wins - wins + losses - leader_losses) / 2
ranked as (
    select
        w.team,
        w.wins,
        w.losses,
        w.w_l_pct,
        c.conference,
        row_number() over (partition by c.conference order by w.wins desc, w.losses asc) as conf_rank
    from with_pct w
    join conference c on w.team = c.team_abbrev
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
    round(
        (l.leader_wins - r.wins + r.losses - l.leader_losses)::double / 2.0,
        1
    ) as gb
from ranked r
join leader l on r.conference = l.conference
order by r.conference, r.conf_rank

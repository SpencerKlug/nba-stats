-- Raw games: one row per game. Light cleanup for marts.
-- Columns from Basketball-Reference schedule: Date, Start (ET), Visitor/Neutral, PTS, Home/Neutral, PTS, Attend., Notes
select
    date,
    start_et,
    visitor_neutral as visitor,
    pts as visitor_pts,
    home_neutral as home,
    pts_1 as home_pts,
    attend as attendance,
    notes,
    season
from {{ source("raw", "games") }}
where date is not null
  and trim(cast(date as varchar)) != ''

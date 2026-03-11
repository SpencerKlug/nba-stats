# Standard Imports
import datetime

# Local Imports
from fetch import ncaa_session
from schedules import get_contest_ids
from team_page import get_team_page
from teams import get_teams


def initial_load():
    """Initial backfill of contest IDs for all teams and academic years."""
    start_date = datetime.date(2000, 1, 1)
    end_date = datetime.date(2026, 1, 1)
    year_list = list(range(start_date.year, end_date.year + 1))
    with ncaa_session() as get_html:
        teams = get_teams(get_html)
        for academic_year in year_list:
            yearly_contest_ids: set[str] = set()
            for team in teams:
                team_page = get_team_page(get_html, team["team_id"], str(academic_year))
                contest_ids = get_contest_ids(team_page)
                yearly_contest_ids.update(contest_ids)

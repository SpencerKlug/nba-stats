# Standard Imports
import datetime

# Local Imports
from teams import get_teams
from schedules import get_contest_ids
from team_page import get_team_page


def initial_load():
    """Iniitla backfill of contest IDs for all teams and academic years."""
    start_date = datetime.date(2000, 1, 1)
    end_date = datetime.date(2026, 1, 1)
    year_list = list(range(start_date.year, end_date.year + 1))
    teams = get_teams()
    for academic_year in year_list:
        yearly_contest_ids = set[str]()
        for team in teams:
            team_page = get_team_page(team["team_id"], academic_year)
            contest_ids = get_contest_ids(team_page)
            yearly_contest_ids.update(contest_ids)

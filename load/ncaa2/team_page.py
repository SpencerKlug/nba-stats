"""Get the schedules/games for each team in the NCAA."""

from constants import NCAA_BASE, NCAA_HEADERS, division, sport_code
from bs4 import BeautifulSoup
import requests

team_id = "590640"  # Gonzaga
academic_year = "2023"


def get_team_page(session: requests.Session, team_id: str, academic_year: str) -> BeautifulSoup:
    """This returns the page for a given team for a given academic year.

    Args:
        session (requests.Session): The session to use to make the request.
        team_id (str): The team ID to get the page for.
        academic_year (str): The academic year to get the page for.

    Returns:
        BeautifulSoup: The BeautifulSoup object for the page.
    """
    url = f"{NCAA_BASE}/teams/{team_id}"
    params = {
        "division": division,
        "sport_code": sport_code,
        "academic_year": academic_year,
    }
    response = session.get(url, params=params)
    soup = BeautifulSoup(response.content, "lxml")
    return soup

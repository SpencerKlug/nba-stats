"""Get the schedules/games for each team in the NCAA."""

from collections.abc import Callable

from bs4 import BeautifulSoup

from constants import NCAA_BASE, division, sport_code

team_id = "590640"  # Gonzaga
academic_year = "2023"


def get_team_page(get_html: Callable[[str, dict], str], team_id: str, academic_year: str) -> BeautifulSoup:
    """This returns the page for a given team for a given academic year.

    Args:
        get_html: Fetcher function (url, params) -> html string. Use fetch.ncaa_session().
        team_id: The team ID to get the page for.
        academic_year: The academic year to get the page for.

    Returns:
        BeautifulSoup: The BeautifulSoup object for the page.
    """
    url = f"{NCAA_BASE}/teams/{team_id}"
    params = {
        "division": division,
        "sport_code": sport_code,
        "academic_year": academic_year,
    }
    html = get_html(url, params)
    return BeautifulSoup(html, "lxml")

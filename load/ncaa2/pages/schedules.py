"""Get the schedules/games for each team in the NCAA."""

import re

from bs4 import BeautifulSoup
from constants import NCAA_BASE, division, sport_code


def get_contest_ids(soup: BeautifulSoup) -> list[str]:
    """This parses the contest IDs from the BeautifulSoup object. This is reliant on the team page.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object to parse the contest IDs from.

    Returns:
        list[str]: The contest IDs as a list of strings.
    """
    links = soup.find_all("a", href=True)
    return [
        m.group(1) for link in links if (m := re.search(r"/contests/(\d+)/box_score", link["href"]))
    ]


if __name__ == "__main__":
    from fetch import ncaa_session

    academic_year = "2023"
    params = {
        "division": division,
        "sport_code": sport_code,
        "academic_year": academic_year,
    }
    url = f"{NCAA_BASE}/teams/609744"
    with ncaa_session() as get_html:
        html = get_html(url, params)
        soup = BeautifulSoup(html, "lxml")
        contest_ids = get_contest_ids(soup)
        print(contest_ids)

"""Get the schedules/games for each team in the NCAA."""

from constants import NCAA_BASE, NCAA_HEADERS, division, sport_code
from bs4 import BeautifulSoup
import requests


def get_contest_ids(soup: BeautifulSoup) -> list[dict]:
    """This parses the contest IDs from the BeautifulSoup object. This is reliant on the team page.

    Args:
        soup (BeautifulSoup): The BeautifulSoup object to parse the contest IDs from.

    Returns:
        list[dict]: The contest IDs as a list of dictionaries.
    """
    links = soup.find_all("a", href=True)
    contest_ids = [link["href"].split("/")[-1] for link in links if "/contests/" in link["href"]]
    return contest_ids


if __name__ == "__main__":
    academic_year = "2023"
    params = {
        "division": division,
        "sport_code": sport_code,
        "academic_year": academic_year,
    }
    url = f"{NCAA_BASE}/teams/609744"
    print(url)
    response = requests.get(url, params=params, headers=NCAA_HEADERS)
    soup = BeautifulSoup(response.content, "lxml")
    print(soup.prettify())
    contest_ids = get_contest_ids(soup)
    print(contest_ids)

"""Get the schedules/games for each team in the NCAA."""

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
    contest_ids = [link["href"].split("/")[-1] for link in links if "/contests/" in link["href"]]
    return contest_ids


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
        print(soup.prettify())
        contest_ids = get_contest_ids(soup)
        print(contest_ids)

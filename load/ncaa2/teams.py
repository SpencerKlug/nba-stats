import pandas as pd
from collections.abc import Callable

from bs4 import BeautifulSoup

from constants import NCAA_BASE, division, sport_code

academic_year = "2023"

params = {
    "division": division,
    "sport_code": sport_code,
    "academic_year": academic_year,
}


def parse_teams(soup: BeautifulSoup) -> list[dict]:
    soup_links = soup.find_all("a", href=True)
    teams = []
    for link in soup_links:
        if "/teams/" in link["href"]:
            college = link.get_text(strip=True)
            team_id = link["href"].split("/")[-1]
            teams.append(
                {
                    "college": college,
                    "team_id": team_id,
                    "link": link["href"],
                }
            )
    return teams


def get_teams(get_html: Callable[[str, dict], str]) -> list[dict]:
    """Fetch team list. get_html from fetch.ncaa_session()."""
    html = get_html(f"{NCAA_BASE}/team/inst_team_list", params)
    soup = BeautifulSoup(html, "lxml")
    return parse_teams(soup)


def main():
    from fetch import ncaa_session

    with ncaa_session() as get_html:
        teams = get_teams(get_html)
        df = pd.DataFrame(teams)
        df.to_csv("teams.csv", index=False)


if __name__ == "__main__":
    main()

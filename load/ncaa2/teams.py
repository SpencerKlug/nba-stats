import pandas as pd
import requests
from bs4 import BeautifulSoup
from constants import NCAA_BASE, NCAA_HEADERS, division, sport_code

academic_year = "2023"

params = {
    "division": division,
    "sport_code": sport_code,
    "academic_year": academic_year,
}


def parse_teams(soup: BeautifulSoup) -> list[dict]:
    soup_links = soup.find_all("a")
    teams = []
    for link in soup_links:
        if "/teams/" in link["href"]:
            college = link.get_text(strip=True)
            team_id = link["href"].split("/")[-1]
            link = link["href"]
            teams.append(
                {
                    "college": college,
                    "team_id": team_id,
                    "link": link,
                }
            )
    return teams


def get_teams(session: requests.Session) -> list[dict]:
    response = session.get(f"{NCAA_BASE}/team/inst_team_list", params=params)
    soup = BeautifulSoup(response.content, "lxml")
    return parse_teams(soup)


def main():
    with requests.Session() as session:
        session.headers.update(NCAA_HEADERS)
        teams = get_teams(session)
        df = pd.DataFrame(teams)
        df.to_csv("teams.csv", index=False)


if __name__ == "__main__":
    main()

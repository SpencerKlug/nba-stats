import requests
from bs4 import BeautifulSoup
import re
from constants import division, sport_code, NCAA_BASE, NCAA_HEADERS
import pandas as pd

academic_year = "2023"

params = {
    "division": division,
    "sport_code": sport_code,
    "academic_year": academic_year,
}


def main():

    # Use a Session (like load.ncaa.core) - NCAA site often requires session cookies
    session = requests.Session()
    session.headers.update(NCAA_HEADERS)
    session.get(NCAA_BASE)  # establish session / cookies
    response = session.get(NCAA_BASE + "/team/inst_team_list", params=params)

    soup = BeautifulSoup(response.content, "lxml")
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
                    "academic_year": academic_year,
                }
            )

    df = pd.DataFrame(teams)
    df.to_csv("teams.csv", index=False)


if __name__ == "__main__":
    main()

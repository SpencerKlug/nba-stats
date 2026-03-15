from bs4 import BeautifulSoup


def parse_play_by_play(soup: BeautifulSoup) -> list[dict]:
    tables = soup.find_all("table")
    play_by_play = []
    for table in tables:
        table_header = soup.find("thead").find_all("span")
        home_team = table_header[0].text.strip()
        away_team = table_header[1].text.strip()
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            cell_length = len(cells)
            if cell_length == 4:
                play_by_play.append(
                    {
                        "home_team": home_team,
                        "away_team": away_team,
                        "time": cells[0].text.strip(),
                        "description": cells[1].text.strip(),
                    }
                )
        return play_by_play

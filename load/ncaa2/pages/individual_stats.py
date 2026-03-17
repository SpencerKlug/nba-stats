import re

from bs4 import BeautifulSoup


def _header_to_key(header: str) -> str:
    """Normalize table header to a valid dict key (lowercase, underscores)."""
    key = re.sub(r"[^\w\s]", " ", header).strip().replace(" ", "_").lower()
    return key or "value"


def _safe_numeric(val: str) -> str | float | int:
    """Coerce to int/float if numeric; otherwise return string."""
    if not val:
        return val
    try:
        if "." in val:
            return float(val)
        return int(val)
    except (ValueError, TypeError):
        return val


def parse_card(card: BeautifulSoup, contest_id: str | None = None) -> list[dict]:
    """Parse a single team's stats card into a list of row dicts.

    Args:
        card: A div.card element containing an h3 (team name) and table.
        contest_id: Optional contest ID to add to each row.
    """
    title_el = card.find("h3")
    team_name = title_el.get_text(strip=True) if title_el else ""

    table = card.find("table")
    if not table:
        return []

    rows = table.find_all("tr")
    if len(rows) < 2:
        return []

    header_cells = rows[0].find_all(["th", "td"])
    headers = []
    for i, c in enumerate(header_cells):
        raw = c.get_text(strip=True)
        headers.append(_header_to_key(raw) if raw else f"col_{i}")

    result = []
    for tr in rows[1:]:
        cells = tr.find_all(["td", "th"])
        vals = [c.get_text(strip=True) for c in cells]
        first_val = vals[0] if vals else ""
        if first_val and first_val.upper() in ("TOTAL", "TOTALS", "TEAM"):
            continue

        row_dict: dict = {"team": team_name}
        if contest_id is not None:
            row_dict["contest_id"] = contest_id

        for i, h in enumerate(headers[: len(vals)]):
            row_dict[h] = _safe_numeric(vals[i]) if i < len(vals) else None
        result.append(row_dict)

    return result


def parse_individual_stats(soup: BeautifulSoup, contest_id: str | None = None) -> list[dict]:
    """Parse all individual stats cards from an NCAA individual_stats page.

    Returns a flat list of dicts, one per player row, with keys for each column
    (team, contest_id if provided, plus normalized header names like min, pts, reb).
    """
    cards = soup.find_all("div", class_="card")
    all_rows = []
    for card in cards:
        all_rows.extend(parse_card(card, contest_id=contest_id))
    return all_rows


if __name__ == "__main__":
    import sys
    from pathlib import Path

    # Add parent dir so `fetch` and `constants` are importable when run as script
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    from fetch import ncaa_session
    from constants import NCAA_BASE

    contest_id = "6470288"
    url = f"{NCAA_BASE}/contests/{contest_id}/individual_stats"
    with ncaa_session() as get_html:
        html = get_html(url)
        soup = BeautifulSoup(html, "lxml")
        rows = parse_individual_stats(soup, contest_id=contest_id)
        for row in rows:
            print(row)
        print(f"... {len(rows)} total rows")

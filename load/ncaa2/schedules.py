"""Get the schedules/games for each team in the NCAA."""

from constants import NCAA_BASE, NCAA_HEADERS, division, sport_code

team_id = "590640"  # Gonzaga
academic_year = "2023"

params = {
    "division": division,
    "sport_code": sport_code,
    "academic_year": academic_year,
}

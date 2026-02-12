# Standard Imports
from __future__ import annotations
import json

# 3rd Party Imports
import requests as r


API_URL = "https://stats.nba.com/stats/{endpoint}"
STATS_HEADERS = {
    "Host": "stats.nba.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://stats.nba.com/",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "Sec-Ch-Ua": '"Chromium";v="140", "Google Chrome";v="140", "Not;A=Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Fetch-Dest": "empty",
}


def call_api(endpoint: str) -> json:
    """Call the NBA API endpoint

    Args:
        endpoint (str): The suffix of the endpoint.

    Returns:
        json: response from the API endpoint
    """
    return r.get(API_URL.format(endpoint=endpoint), headers=STATS_HEADERS).json()


def main() -> None:
    endpoint = "teams"
    endpoint_data = call_api(endpoint=endpoint)
    with open("sample.json", "w") as f:
        json.dump(endpoint_data, f, indent=2)


if __name__ == "__main__":
    main()

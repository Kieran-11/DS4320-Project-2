"""
01_scrape_games.py
------------------
Scrapes neutral-site NCAAB game results from Sports-Reference (cbbref).
Collects one season at a time using their schedule/results pages.
Outputs: data/raw_games.csv

Dependencies: pip install requests beautifulsoup4 pandas
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

# ── Configuration ──────────────────────────────────────────────────────────────
SEASONS = list(range(2015, 2025))   # 2015 through 2024 (adjust as needed)
OUTPUT_PATH = "data/raw_games.csv"
SLEEP_SECONDS = 4   # be polite to Sports-Reference; they rate-limit aggressively
# ───────────────────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def scrape_season(year: int) -> pd.DataFrame:
    """
    Fetches the full game results page for a given season from Sports-Reference.
    The 'year' parameter refers to the ending year of the season (e.g. 2024 = 2023-24).

    Sports-Reference marks neutral-site games with 'N' in the location column.
    URL pattern: https://www.sports-reference.com/cbb/seasons/men/{year}-schedule.html
    """
    url = f"https://www.sports-reference.com/cbb/seasons/men/{year}-schedule.html"
    print(f"  Fetching {year} schedule: {url}")

    resp = requests.get(url, headers=HEADERS, timeout=30)
    if resp.status_code != 200:
        print(f"  WARNING: Got status {resp.status_code} for year {year}. Skipping.")
        return pd.DataFrame()

    soup = BeautifulSoup(resp.text, "html.parser")

    # The schedule table has id="schedule"
    table = soup.find("table", {"id": "schedule"})
    if table is None:
        print(f"  WARNING: Could not find schedule table for {year}.")
        return pd.DataFrame()

    rows = []
    tbody = table.find("tbody")
    for tr in tbody.find_all("tr"):
        # Skip header rows that repeat mid-table
        if tr.get("class") and "thead" in tr.get("class"):
            continue

        cells = tr.find_all(["td", "th"])
        if not cells or len(cells) < 8:
            continue

        try:
            # Columns (0-indexed): date, time, winner, pts, (neutral/@/blank), loser, pts, ...
            date_cell   = tr.find("td", {"data-stat": "date_game"})
            winner_cell = tr.find("td", {"data-stat": "winner_school_name"})
            loser_cell  = tr.find("td", {"data-stat": "loser_school_name"})
            pts_w_cell  = tr.find("td", {"data-stat": "winner_pts"})
            pts_l_cell  = tr.find("td", {"data-stat": "loser_pts"})
            loc_cell    = tr.find("td", {"data-stat": "game_location"})

            if not all([date_cell, winner_cell, loser_cell]):
                continue

            location_flag = loc_cell.get_text(strip=True) if loc_cell else ""

            # Only keep neutral-site games ("N" in location column)
            if location_flag != "N":
                continue

            row = {
                "season":     year,
                "date":       date_cell.get_text(strip=True),
                "winner":     winner_cell.get_text(strip=True),
                "loser":      loser_cell.get_text(strip=True),
                "winner_pts": pts_w_cell.get_text(strip=True) if pts_w_cell else None,
                "loser_pts":  pts_l_cell.get_text(strip=True) if pts_l_cell else None,
                "location":   location_flag,
            }
            rows.append(row)

        except Exception as e:
            print(f"  Row parse error: {e}")
            continue

    df = pd.DataFrame(rows)
    print(f"  → {len(df)} neutral-site games found for {year}")
    return df


def main():
    os.makedirs("data", exist_ok=True)
    all_seasons = []

    for year in SEASONS:
        df = scrape_season(year)
        if not df.empty:
            all_seasons.append(df)
        time.sleep(SLEEP_SECONDS)   # rate-limit courtesy pause

    if not all_seasons:
        print("No data collected. Check your network or year range.")
        return

    combined = pd.concat(all_seasons, ignore_index=True)

    # Basic type cleanup
    combined["winner_pts"] = pd.to_numeric(combined["winner_pts"], errors="coerce")
    combined["loser_pts"]  = pd.to_numeric(combined["loser_pts"],  errors="coerce")
    combined["point_diff"] = combined["winner_pts"] - combined["loser_pts"]
    combined["date"]       = pd.to_datetime(combined["date"], errors="coerce")

    combined.to_csv(OUTPUT_PATH, index=False)
    print(f"\nSaved {len(combined)} neutral-site games to {OUTPUT_PATH}")
    print(combined.head())


if __name__ == "__main__":
    main()

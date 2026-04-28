"""
01_scrape_games.py
------------------
Pulls neutral-site NCAAB game results from Barttorvik's game log endpoint.

Replaces the original Sports-Reference scraper, which now returns 403/404
for automated requests. Barttorvik provides the same neutral-site game data
via a free JSON API with no key required.

Endpoint: https://barttorvik.com/getgames.php?year=YYYY&neutralonly=1&json=1

Outputs: data/raw_games.csv

Dependencies: pip install requests pandas
"""

import requests
import pandas as pd
import time
import os
import logging
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────────
SEASONS     = list(range(2015, 2025))   # 2014-15 through 2023-24
OUTPUT_PATH = "data/raw_games.csv"
SLEEP_SECONDS = 2                        # be polite to barttorvik

# ── Logging ────────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/01_scrape_games.log", mode="w"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("scrape_games")

# ── Barttorvik game column mapping ─────────────────────────────────────────────
# The JSON endpoint returns a list of lists. Column order as of 2024:
# [date, away, away_pts, home, home_pts, ot, location_flag, ...]
# location_flag: 'N' = neutral, '' = home game, '@' = away
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

def fetch_season(year: int) -> pd.DataFrame:
    """
    Fetch all neutral-site games for a given season from Barttorvik.

    The 'year' parameter is the ending year of the season
    (e.g. 2024 = 2023-24 season).

    Parameters
    ----------
    year : int
        Season ending year (2015-2024).

    Returns
    -------
    pd.DataFrame
        One row per neutral-site game with winner, loser, scores, season.
    """
    url = f"https://barttorvik.com/getgames.php?year={year}&neutralonly=1&json=1"
    log.info(f"Fetching {year}: {url}")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.HTTPError as e:
        log.warning(f"  HTTP error for {year}: {e}")
        return pd.DataFrame()
    except Exception as e:
        log.warning(f"  Error fetching {year}: {e}")
        return pd.DataFrame()

    if not data:
        log.warning(f"  Empty response for {year}")
        return pd.DataFrame()

    # Parse each game row
    # Barttorvik game row structure (confirmed via inspection):
    # index 0: date (YYYYMMDD string)
    # index 1: team1 name
    # index 2: team1 score
    # index 3: team2 name
    # index 4: team2 score
    # index 5: number of overtimes
    # index 6: location flag ('N' for neutral)
    rows = []
    for game in data:
        try:
            if not isinstance(game, list) or len(game) < 7:
                continue

            date_raw  = str(game[0])
            team1     = str(game[1])
            pts1      = game[2]
            team2     = str(game[3])
            pts2      = game[4]
            location  = str(game[6]) if len(game) > 6 else ""

            # Only keep neutral-site games
            if location != "N":
                continue

            # Determine winner/loser
            try:
                pts1 = float(pts1)
                pts2 = float(pts2)
            except (TypeError, ValueError):
                continue

            if pts1 > pts2:
                winner, loser   = team1, team2
                winner_pts, loser_pts = pts1, pts2
            elif pts2 > pts1:
                winner, loser   = team2, team1
                winner_pts, loser_pts = pts2, pts1
            else:
                continue  # skip ties (shouldn't exist in CBB)

            # Parse date
            try:
                date = pd.to_datetime(date_raw, format="%Y%m%d").strftime("%Y-%m-%d")
            except Exception:
                date = date_raw

            rows.append({
                "season":      year,
                "date":        date,
                "winner":      winner,
                "loser":       loser,
                "winner_pts":  winner_pts,
                "loser_pts":   loser_pts,
                "point_diff":  winner_pts - loser_pts,
                "location":    "N",
            })

        except Exception as e:
            log.debug(f"  Row parse error: {e}")
            continue

    df = pd.DataFrame(rows)
    log.info(f"  → {len(df)} neutral-site games for {year}")
    return df


def main():
    """
    Main entry point. Fetches all seasons and writes combined CSV.
    Logs progress and a summary on completion.
    """
    os.makedirs("data", exist_ok=True)

    all_seasons = []
    for year in SEASONS:
        df = fetch_season(year)
        if not df.empty:
            all_seasons.append(df)
        time.sleep(SLEEP_SECONDS)

    if not all_seasons:
        log.error("No data collected. Check network or barttorvik endpoint.")
        return

    combined = pd.concat(all_seasons, ignore_index=True)
    combined["winner_pts"] = pd.to_numeric(combined["winner_pts"], errors="coerce")
    combined["loser_pts"]  = pd.to_numeric(combined["loser_pts"],  errors="coerce")
    combined["point_diff"] = combined["winner_pts"] - combined["loser_pts"]

    combined.to_csv(OUTPUT_PATH, index=False)

    log.info(f"\n{'='*50}")
    log.info(f"Saved {len(combined):,} neutral-site games to {OUTPUT_PATH}")
    log.info(f"Seasons covered: {sorted(combined['season'].unique())}")
    log.info(f"Games per season:\n{combined.groupby('season').size().to_string()}")
    print(combined.head(10).to_string())


if __name__ == "__main__":
    main()

"""
01_scrape_games.py
------------------
Processes manually downloaded NCAA West Region tournament game results.

Data source: Sports-Reference CBB Play Index, exported manually via browser
(automated scraping is blocked). The CSV contains West Region NCAA Tournament
games from 2015-2025, with each game appearing twice (once per team).

This script deduplicates, standardizes team names, and outputs one row
per game in winner/loser format matching the rest of the pipeline.

Input:  data/West_results.csv   (manually downloaded from Sports-Reference)
Output: data/raw_games.csv

Dependencies: pip install pandas
"""

import pandas as pd
import os
import logging
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────────
INPUT_PATH  = "data/West_results.csv"
OUTPUT_PATH = "data/raw_games.csv"

# Known West Region venue cities by year.
# Source: NCAA public tournament bracket records.
# Used by script 03 to geocode the actual game venue instead of estimating.
WEST_VENUES = {
    2015: ("Jacksonville",        30.3322, -81.6557),
    2016: ("Sacramento",          38.5816, -121.4944),
    2017: ("Salt Lake City",      40.7608, -111.8910),
    2018: ("San Diego",           32.7157, -117.1611),
    2019: ("San Jose",            37.3382, -121.8863),
    2021: ("Indianapolis",        39.7684,  -86.1581),   # bubble year, all sites in Indy
    2022: ("San Francisco",       37.7749, -122.4194),
    2023: ("Las Vegas",           36.1699, -115.1398),
    2024: ("Los Angeles",         34.0522, -118.2437),
    2025: ("San Francisco",       37.7749, -122.4194),
}

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


def standardize_name(name: str) -> str:
    """
    Normalize team names to match Barttorvik naming conventions.
    Sports-Reference uses some abbreviations that differ from Barttorvik.
    """
    mapping = {
        "Arizona St.":       "Arizona State",
        "Michigan St.":      "Michigan State",
        "Florida St.":       "Florida State",
        "Ohio St.":          "Ohio State",
        "Kansas St.":        "Kansas State",
        "Iowa St.":          "Iowa State",
        "Colorado St.":      "Colorado State",
        "Oregon St.":        "Oregon State",
        "Washington St.":    "Washington State",
        "Brigham Young":     "BYU",
        "UNC Wilmington":    "UNCW",
        "Loyola Chicago":    "Loyola-Chicago",
        "St. Mary's (CA)":   "Saint Mary's (CA)",
        "Saint Mary's":      "Saint Mary's (CA)",
        "Connecticut":       "UConn",
        "North Carolina":    "UNC",
        "Southern California": "USC",
        "Central Florida":   "UCF",
        "Nevada-Las Vegas":  "UNLV",
        "Virginia Commonwealth": "VCU",
        "Texas-El Paso":     "UTEP",
        "Texas-San Antonio": "UTSA",
        "Florida International": "FIU",
        "Florida Atlantic":  "FAU",
        "Mississippi":       "Ole Miss",
        "Miami (FL)":        "Miami FL",
        "Miami (Ohio)":      "Miami OH",
    }
    return mapping.get(name.strip(), name.strip())


def process_games(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the raw Sports-Reference export (two rows per game) into
    one row per game in winner/loser format.

    The input has W/L from each team's perspective — we keep only the
    W rows to deduplicate, then rename columns to the pipeline standard.

    Parameters
    ----------
    df : pd.DataFrame
        Raw CSV as loaded from Sports-Reference export.

    Returns
    -------
    pd.DataFrame
        One row per game: season, date, winner, loser, scores,
        seeds, point_diff, venue info.
    """
    # Keep only the winner's row to avoid counting each game twice
    wins = df[df["Result"] == "W"].copy()
    log.info(f"Rows after dedup (wins only): {len(wins)}")

    rows = []
    for _, row in wins.iterrows():
        season = int(row["Date"])
        winner = standardize_name(str(row["Team"]))
        loser  = standardize_name(str(row["Opp"]))
        winner_pts = float(row["Team_PTS"])
        loser_pts  = float(row["Opp_PTS"])
        winner_seed = int(row["Seed"])        if pd.notna(row["Seed"])       else None
        loser_seed  = int(row["Opp Seed"])    if pd.notna(row["Opp Seed"])   else None

        venue = WEST_VENUES.get(season)
        venue_city = venue[0] if venue else None
        venue_lat  = venue[1] if venue else None
        venue_lon  = venue[2] if venue else None

        rows.append({
            "season":       season,
            "date":         f"{season}-03-01",   # exact date not in export; month is March
            "region":       "West",
            "winner":       winner,
            "loser":        loser,
            "winner_pts":   winner_pts,
            "loser_pts":    loser_pts,
            "winner_seed":  winner_seed,
            "loser_seed":   loser_seed,
            "point_diff":   winner_pts - loser_pts,
            "location":     "N",
            "venue_city":   venue_city,
            "venue_lat":    venue_lat,
            "venue_lon":    venue_lon,
        })

    return pd.DataFrame(rows)


def main():
    """
    Main entry point. Loads the raw CSV, processes it, and writes
    the standardized output to data/raw_games.csv.
    """
    os.makedirs("data", exist_ok=True)

    if not os.path.exists(INPUT_PATH):
        log.error(
            f"Input file not found: {INPUT_PATH}\n"
            "Please copy your downloaded CSV to data/West_results.csv"
        )
        return

    try:
        df = pd.read_csv(INPUT_PATH)
        log.info(f"Loaded {len(df)} rows from {INPUT_PATH}")
    except Exception as e:
        log.error(f"Failed to read CSV: {e}")
        raise

    games = process_games(df)
    games.to_csv(OUTPUT_PATH, index=False)

    log.info(f"Saved {len(games)} games to {OUTPUT_PATH}")
    log.info(f"Seasons: {sorted(games['season'].unique())}")
    log.info(f"Games per season:\n{games.groupby('season').size().to_string()}")
    print(games.head(10).to_string())


if __name__ == "__main__":
    main()

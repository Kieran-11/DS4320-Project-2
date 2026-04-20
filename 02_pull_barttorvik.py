"""
02_pull_barttorvik.py
---------------------
Pulls team-season efficiency ratings from Barttorvik (T-Rank).
Free, no API key required. Returns JSON directly from their endpoint.
Outputs: data/barttorvik_ratings.csv

Dependencies: pip install requests pandas
"""

import requests
import pandas as pd
import time
import os

# ── Configuration ──────────────────────────────────────────────────────────────
SEASONS = list(range(2015, 2025))   # match whatever years you scraped in 01_
OUTPUT_PATH = "data/barttorvik_ratings.csv"
SLEEP_SECONDS = 2
# ───────────────────────────────────────────────────────────────────────────────

# Barttorvik column names returned by the JSON endpoint (as of 2024).
# The JSON returns a list of lists, so we name columns manually.
COLUMN_NAMES = [
    "rank",
    "team",
    "conf",
    "record",
    "adjoe",        # Adjusted Offensive Efficiency (points per 100 possessions)
    "adjde",        # Adjusted Defensive Efficiency (points allowed per 100)
    "barthag",      # Power rating (prob of beating avg D1 team)
    "efg_pct",      # Effective FG%
    "efg_d_pct",    # Effective FG% allowed
    "tor",          # Turnover rate
    "tord",         # Turnover rate forced
    "orb",          # Offensive rebound rate
    "drb",          # Defensive rebound rate
    "ftr",          # Free throw rate
    "ftrd",         # Free throw rate allowed
    "two_pt_pct",   # 2-point %
    "two_pt_d_pct", # 2-point % allowed
    "three_pt_pct", # 3-point %
    "three_pt_d_pct",# 3-point % allowed
    "adj_tempo",    # Adjusted tempo (possessions per 40 min)
    "wab",          # Wins above bubble
]


def get_barttorvik_season(year: int) -> pd.DataFrame:
    """
    Fetches T-Rank data for all D1 teams in a given season.
    'year' = ending year of the season (e.g. 2024 = 2023-24 season).
    """
    url = f"https://barttorvik.com/trank.php?year={year}&json=1"
    print(f"  Fetching Barttorvik {year}: {url}")

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ERROR fetching {year}: {e}")
        return pd.DataFrame()

    # data is a list of lists; each inner list is one team
    records = []
    for row in data:
        if not isinstance(row, list):
            continue
        # Pad or trim to expected column count
        padded = (row + [None] * len(COLUMN_NAMES))[:len(COLUMN_NAMES)]
        records.append(dict(zip(COLUMN_NAMES, padded)))

    df = pd.DataFrame(records)
    df["season"] = year
    print(f"  → {len(df)} teams for {year}")
    return df


def main():
    os.makedirs("data", exist_ok=True)
    all_seasons = []

    for year in SEASONS:
        df = get_barttorvik_season(year)
        if not df.empty:
            all_seasons.append(df)
        time.sleep(SLEEP_SECONDS)

    if not all_seasons:
        print("No data collected.")
        return

    combined = pd.concat(all_seasons, ignore_index=True)

    # Numeric coercion
    numeric_cols = [
        "adjoe", "adjde", "barthag", "efg_pct", "efg_d_pct",
        "tor", "tord", "orb", "drb", "ftr", "ftrd",
        "two_pt_pct", "two_pt_d_pct", "three_pt_pct", "three_pt_d_pct",
        "adj_tempo", "wab", "rank"
    ]
    for col in numeric_cols:
        if col in combined.columns:
            combined[col] = pd.to_numeric(combined[col], errors="coerce")

    combined.to_csv(OUTPUT_PATH, index=False)
    print(f"\nSaved {len(combined)} team-season rows to {OUTPUT_PATH}")
    print(combined[["season", "team", "adjoe", "adjde", "barthag", "adj_tempo"]].head(10))


if __name__ == "__main__":
    main()

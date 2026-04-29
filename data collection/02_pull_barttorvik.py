"""
02_pull_barttorvik.py
---------------------
Loads team efficiency ratings from the Kaggle college basketball dataset
(cbb.csv) instead of scraping Barttorvik directly (which now blocks
automated requests with a browser verification challenge).

The cbb.csv dataset contains the same Barttorvik-derived metrics:
ADJOE, ADJDE, BARTHAG, EFG_O, EFG_D, TOR, TORD, ORB, DRB, ADJ_T, WAB
for every D-I team from 2013-2023.

Input:  data/cbb.csv         (downloaded from Kaggle: andrewsundberg/college-basketball-dataset)
Output: data/barttorvik_ratings.csv

Dependencies: pip install pandas
"""

import pandas as pd
import os
import logging
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────────
INPUT_PATH  = "data/cbb.csv"
OUTPUT_PATH = "data/barttorvik_ratings.csv"

# ── Logging ────────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/02_pull_barttorvik.log", mode="w"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("pull_barttorvik")


def load_and_clean(path: str) -> pd.DataFrame:
    """
    Load cbb.csv and standardize column names to match the rest of the
    pipeline (lowercase, barttorvik naming conventions).

    The Kaggle dataset uses uppercase column names (ADJOE, ADJDE, etc.)
    and calls the season column YEAR. We rename everything to match
    what scripts 03, 04, and 05 expect.

    Parameters
    ----------
    path : str
        Path to cbb.csv.

    Returns
    -------
    pd.DataFrame
        Cleaned ratings with standardized column names.
    """
    try:
        df = pd.read_csv(path)
        log.info(f"Loaded {len(df):,} rows from {path}")
    except FileNotFoundError:
        log.error(f"File not found: {path}. Make sure cbb.csv is in your data/ folder.")
        raise

    # Rename columns to match pipeline conventions
    df = df.rename(columns={
        "TEAM":     "team",
        "CONF":     "conf",
        "G":        "games",
        "W":        "wins",
        "ADJOE":    "adjoe",
        "ADJDE":    "adjde",
        "BARTHAG":  "barthag",
        "EFG_O":    "efg_pct",
        "EFG_D":    "efg_d_pct",
        "TOR":      "tor",
        "TORD":     "tord",
        "ORB":      "orb",
        "DRB":      "drb",
        "FTR":      "ftr",
        "FTRD":     "ftrd",
        "2P_O":     "two_p_o",
        "2P_D":     "two_p_d",
        "3P_O":     "three_p_o",
        "3P_D":     "three_p_d",
        "ADJ_T":    "adj_tempo",
        "WAB":      "wab",
        "POSTSEASON": "postseason",
        "SEED":     "seed",
        "YEAR":     "season",
    })

    # Convert numeric columns
    numeric_cols = ["adjoe", "adjde", "barthag", "efg_pct", "efg_d_pct",
                    "tor", "tord", "orb", "drb", "adj_tempo", "wab", "season"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Add a rank column based on barthag within each season
    df["rank"] = df.groupby("season")["barthag"].rank(ascending=False).astype(int)

    # Drop rows missing key fields
    before = len(df)
    df = df.dropna(subset=["team", "season", "barthag", "adjoe", "adjde"])
    log.info(f"Dropped {before - len(df)} rows with missing key fields")

    return df


def main():
    """
    Main entry point. Loads cbb.csv, cleans it, and writes
    the standardized ratings CSV.
    """
    os.makedirs("data", exist_ok=True)

    df = load_and_clean(INPUT_PATH)

    log.info(f"Seasons covered: {sorted(df['season'].unique())}")
    log.info(f"Teams per season:\n{df.groupby('season').size().to_string()}")

    df.to_csv(OUTPUT_PATH, index=False)
    log.info(f"Saved {len(df):,} team-season ratings to {OUTPUT_PATH}")
    print(df.head(10).to_string())


if __name__ == "__main__":
    main()

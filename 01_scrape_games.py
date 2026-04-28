"""
01_scrape_games.py
------------------
Pulls neutral-site NCAAB game results using the sportsreference package.

Replaces web scraping entirely — sportsreference wraps Sports-Reference data
locally without making live HTTP requests for historical seasons.

Outputs: data/raw_games.csv

Dependencies: pip install sportsreference pandas
"""

import pandas as pd
import os
import logging
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────────
SEASONS     = list(range(2015, 2025))
OUTPUT_PATH = "data/raw_games.csv"

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


def fetch_season(year: int) -> pd.DataFrame:
    """
    Fetch all neutral-site games for a given season using sportsreference.

    Parameters
    ----------
    year : int
        Season ending year (e.g. 2024 = 2023-24 season).

    Returns
    -------
    pd.DataFrame
        One row per neutral-site game with winner, loser, scores, season.
    """
    try:
        from sportsreference.ncaab.schedule import Schedule
        from sportsreference.ncaab.teams import Teams
    except ImportError:
        log.error("sportsreference not installed. Run: pip install sportsreference")
        raise

    log.info(f"Fetching {year} season...")
    rows = []

    try:
        teams = Teams(year=str(year))
        team_list = list(teams)
        log.info(f"  {len(team_list)} teams found for {year}")
    except Exception as e:
        log.warning(f"  Could not load teams for {year}: {e}")
        return pd.DataFrame()

    seen = set()  # deduplicate — each game appears in both teams' schedules

    for team in team_list:
        try:
            schedule = Schedule(team.abbreviation, year=str(year))
        except Exception as e:
            log.debug(f"  Schedule error for {team.abbreviation}: {e}")
            continue

        for game in schedule:
            try:
                # Only neutral-site games
                if game.location != "Neutral":
                    continue

                # Build a dedup key so we don't add the same game twice
                date_str = str(game.date)
                opp = game.opponent_abbreviation or game.opponent_name or ""
                key = tuple(sorted([team.abbreviation, opp, date_str]))
                if key in seen:
                    continue
                seen.add(key)

                pts_scored   = game.points
                pts_opponent = game.opponent_points

                if pts_scored is None or pts_opponent is None:
                    continue

                # Determine winner/loser
                if pts_scored > pts_opponent:
                    winner     = team.name
                    loser      = game.opponent_name or opp
                    winner_pts = pts_scored
                    loser_pts  = pts_opponent
                elif pts_opponent > pts_scored:
                    winner     = game.opponent_name or opp
                    loser      = team.name
                    winner_pts = pts_opponent
                    loser_pts  = pts_scored
                else:
                    continue  # no ties in CBB

                rows.append({
                    "season":     year,
                    "date":       date_str,
                    "winner":     winner,
                    "loser":      loser,
                    "winner_pts": float(winner_pts),
                    "loser_pts":  float(loser_pts),
                    "point_diff": float(winner_pts) - float(loser_pts),
                    "location":   "N",
                })

            except Exception as e:
                log.debug(f"  Game parse error: {e}")
                continue

    df = pd.DataFrame(rows)
    log.info(f"  → {len(df)} neutral-site games for {year}")
    return df


def main():
    """
    Main entry point. Fetches all seasons and writes combined CSV.
    """
    os.makedirs("data", exist_ok=True)

    all_seasons = []
    for year in SEASONS:
        try:
            df = fetch_season(year)
            if not df.empty:
                all_seasons.append(df)
        except Exception as e:
            log.error(f"Failed season {year}: {e}")
            continue

    if not all_seasons:
        log.error("No data collected.")
        return

    combined = pd.concat(all_seasons, ignore_index=True)
    combined["winner_pts"] = pd.to_numeric(combined["winner_pts"], errors="coerce")
    combined["loser_pts"]  = pd.to_numeric(combined["loser_pts"],  errors="coerce")
    combined["point_diff"] = combined["winner_pts"] - combined["loser_pts"]

    combined.to_csv(OUTPUT_PATH, index=False)

    log.info(f"\n{'='*50}")
    log.info(f"Saved {len(combined):,} neutral-site games to {OUTPUT_PATH}")
    log.info(f"Seasons: {sorted(combined['season'].unique())}")
    log.info(f"Games per season:\n{combined.groupby('season').size().to_string()}")
    print(combined.head(10).to_string())


if __name__ == "__main__":
    main()

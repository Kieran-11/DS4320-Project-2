"""
04_merge_and_ingest.py
----------------------
Merges game-distance data with Barttorvik team ratings and loads the
final documents into MongoDB Atlas.

The document structure is designed around the West Region hypothesis:
each document stores both teams' longitudes, the longitude difference
(key hypothesis variable), travel distances, and Barttorvik ratings.

Inputs:
    data/games_with_distance.csv   (from 03_compute_distances.py)
    data/barttorvik_ratings.csv    (from 02_pull_barttorvik.py)
Output:
    MongoDB: ncaab_west_region.games
    MongoDB: ncaab_west_region.team_ratings

Dependencies: pip install pymongo pandas
"""

import os
import pandas as pd
import numpy as np
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import logging
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────────
MONGO_URI   = os.environ.get("MONGO_URI", "YOUR_CONNECTION_STRING")
DB_NAME     = "ncaab_west_region"
GAMES_COL   = "games"
RATINGS_COL = "team_ratings"
GAMES_PATH   = "data/games_with_distance.csv"
RATINGS_PATH = "data/barttorvik_ratings.csv"

# ── Logging ────────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/04_merge_and_ingest.log", mode="w"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("ingest")


def clean_nan(obj):
    """Recursively replace float NaN with None for MongoDB compatibility."""
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan(i) for i in obj]
    elif isinstance(obj, float) and np.isnan(obj):
        return None
    return obj


def merge_ratings(games: pd.DataFrame, ratings: pd.DataFrame) -> pd.DataFrame:
    """
    Join Barttorvik season ratings onto game rows for both winner and loser.

    Matches on (team, season). Adds derived features including the key
    hypothesis variables: longitude difference and seed difference.

    Parameters
    ----------
    games   : Game-level DataFrame from script 03.
    ratings : Team-season ratings from script 02.

    Returns
    -------
    pd.DataFrame : Games with ratings columns for both teams.
    """
    rating_cols = [
        "team", "season", "rank", "adjoe", "adjde", "barthag",
        "adj_tempo", "wab", "efg_pct", "efg_d_pct",
        "tor", "tord", "orb", "drb",
    ]
    rat = ratings[rating_cols].copy()

    # Join winner ratings
    games = games.merge(
        rat.rename(columns={c: f"winner_{c}" for c in rating_cols if c not in ("team", "season")}),
        how="left",
        left_on=["winner", "season"],
        right_on=["team", "season"],
    ).drop(columns=["team"])

    # Join loser ratings
    games = games.merge(
        rat.rename(columns={c: f"loser_{c}" for c in rating_cols if c not in ("team", "season")}),
        how="left",
        left_on=["loser", "season"],
        right_on=["team", "season"],
    ).drop(columns=["team"])

    # Derived differential features for modeling
    games["adjoe_diff"]    = games["winner_adjoe"]    - games["loser_adjoe"]
    games["adjde_diff"]    = games["winner_adjde"]    - games["loser_adjde"]
    games["barthag_diff"]  = games["winner_barthag"]  - games["loser_barthag"]
    games["tempo_diff"]    = games["winner_adj_tempo"]- games["loser_adj_tempo"]
    games["seed_diff"]     = games["winner_seed"]     - games["loser_seed"]
    # Key hypothesis variable: negative = winner was further west than loser
    games["lon_diff"]      = games["winner_lon"]      - games["loser_lon"]

    return games


def build_game_document(row: dict) -> dict:
    """
    Convert a merged DataFrame row into a structured MongoDB document.

    The document nests team data under 'winner' and 'loser' subdocs,
    with a 'hypothesis' subdoc holding the longitude and distance
    variables central to the research question.

    Parameters
    ----------
    row : dict from DataFrame.iterrows()

    Returns
    -------
    dict : MongoDB-ready document.
    """
    return {
        "season":      row.get("season"),
        "date":        row.get("date"),
        "region":      row.get("region", "West"),
        "neutral_site": True,
        "venue": {
            "city": row.get("venue_city"),
            "lat":  row.get("venue_lat"),
            "lon":  row.get("venue_lon"),
        },
        "winner": {
            "team": row.get("winner"),
            "pts":  row.get("winner_pts"),
            "seed": row.get("winner_seed"),
            "lat":  row.get("winner_lat"),
            "lon":  row.get("winner_lon"),
            "dist_miles": row.get("winner_dist_miles"),
            "ratings": {
                "rank":       row.get("winner_rank"),
                "adjoe":      row.get("winner_adjoe"),
                "adjde":      row.get("winner_adjde"),
                "barthag":    row.get("winner_barthag"),
                "adj_tempo":  row.get("winner_adj_tempo"),
                "wab":        row.get("winner_wab"),
                "efg_pct":    row.get("winner_efg_pct"),
                "efg_d_pct":  row.get("winner_efg_d_pct"),
                "tor":        row.get("winner_tor"),
                "tord":       row.get("winner_tord"),
                "orb":        row.get("winner_orb"),
                "drb":        row.get("winner_drb"),
            },
        },
        "loser": {
            "team": row.get("loser"),
            "pts":  row.get("loser_pts"),
            "seed": row.get("loser_seed"),
            "lat":  row.get("loser_lat"),
            "lon":  row.get("loser_lon"),
            "dist_miles": row.get("loser_dist_miles"),
            "ratings": {
                "rank":       row.get("loser_rank"),
                "adjoe":      row.get("loser_adjoe"),
                "adjde":      row.get("loser_adjde"),
                "barthag":    row.get("loser_barthag"),
                "adj_tempo":  row.get("loser_adj_tempo"),
                "wab":        row.get("loser_wab"),
                "efg_pct":    row.get("loser_efg_pct"),
                "efg_d_pct":  row.get("loser_efg_d_pct"),
                "tor":        row.get("loser_tor"),
                "tord":       row.get("loser_tord"),
                "orb":        row.get("loser_orb"),
                "drb":        row.get("loser_drb"),
            },
        },
        # Hypothesis variables — all the key longitude/distance diffs
        "hypothesis": {
            "lon_diff":           row.get("lon_diff"),        # winner_lon - loser_lon (negative = winner further west)
            "winner_lon_diff":    row.get("winner_lon_diff"), # winner_lon - loser_lon
            "dist_diff_miles":    row.get("dist_diff_miles"), # winner_dist - loser_dist
            "seed_diff":          row.get("seed_diff"),       # winner_seed - loser_seed
            "barthag_diff":       row.get("barthag_diff"),
            "adjoe_diff":         row.get("adjoe_diff"),
            "adjde_diff":         row.get("adjde_diff"),
        },
        "ingested_at": datetime.utcnow(),
    }


def ingest_games(collection, df: pd.DataFrame) -> None:
    """Upsert game documents into MongoDB using season+teams as natural key."""
    ops = []
    for _, row in df.iterrows():
        doc = clean_nan(build_game_document(row.to_dict()))
        filter_key = {
            "season":       doc["season"],
            "winner.team":  doc["winner"]["team"],
            "loser.team":   doc["loser"]["team"],
        }
        ops.append(UpdateOne(filter_key, {"$set": doc}, upsert=True))

    if ops:
        result = collection.bulk_write(ops)
        log.info(f"  Games: {result.upserted_count} new, {result.modified_count} updated")


def ingest_ratings(collection, df: pd.DataFrame) -> None:
    """Upsert team rating documents into MongoDB."""
    ops = []
    for _, row in df.iterrows():
        doc = clean_nan(row.to_dict())
        doc["ingested_at"] = datetime.utcnow()
        filter_key = {"team": doc["team"], "season": doc["season"]}
        ops.append(UpdateOne(filter_key, {"$set": doc}, upsert=True))

    if ops:
        result = collection.bulk_write(ops)
        log.info(f"  Ratings: {result.upserted_count} new, {result.modified_count} updated")


def main():
    """Main entry point. Merges data and loads into MongoDB Atlas."""
    log.info("Loading CSVs...")
    games   = pd.read_csv(GAMES_PATH)
    ratings = pd.read_csv(RATINGS_PATH)
    log.info(f"  Games: {len(games)} | Ratings: {len(ratings)}")

    log.info("Merging ratings onto games...")
    merged = merge_ratings(games, ratings)
    log.info(f"  Merged shape: {merged.shape}")

    log.info(f"Connecting to MongoDB ({DB_NAME})...")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
        client.admin.command("ping")
        log.info("  Connected.")
    except Exception as e:
        log.error(f"  Connection failed: {e}")
        raise

    db = client[DB_NAME]

    log.info(f"Ingesting games → {DB_NAME}.{GAMES_COL}")
    ingest_games(db[GAMES_COL], merged)

    log.info(f"Ingesting ratings → {DB_NAME}.{RATINGS_COL}")
    ingest_ratings(db[RATINGS_COL], ratings)

    # Create indexes for common query patterns
    log.info("Creating indexes...")
    db[GAMES_COL].create_index([("season", 1)])
    db[GAMES_COL].create_index([("winner.team", 1), ("season", 1)])
    db[GAMES_COL].create_index([("hypothesis.lon_diff", 1)])
    db[RATINGS_COL].create_index([("team", 1), ("season", 1)], unique=True)
    log.info("  Indexes created.")

    game_count   = db[GAMES_COL].count_documents({})
    rating_count = db[RATINGS_COL].count_documents({})
    log.info(f"\nDatabase '{DB_NAME}' summary:")
    log.info(f"  {GAMES_COL}:    {game_count} documents")
    log.info(f"  {RATINGS_COL}: {rating_count} documents")

    client.close()
    log.info("Done.")


if __name__ == "__main__":
    main()

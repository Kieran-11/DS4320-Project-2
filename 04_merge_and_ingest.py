"""
04_merge_and_ingest.py
----------------------
Merges game-distance data with Barttorvik team ratings and loads the
final documents into MongoDB Atlas.

Inputs:
  data/games_with_distance.csv     (from 03_compute_distances.py)
  data/barttorvik_ratings.csv      (from 02_pull_barttorvik.py)

Output:
  MongoDB collection: ncaab_neutral_site.games
  MongoDB collection: ncaab_neutral_site.team_ratings

Dependencies: pip install pymongo pandas
"""

import pandas as pd
import numpy as np
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import os

# ── Configuration ──────────────────────────────────────────────────────────────
MONGO_URI       = "YOUR_CONNECTION_STRING"   # ← replace with your Atlas URI
DB_NAME         = "ncaab_neutral_site"
GAMES_COL       = "games"
RATINGS_COL     = "team_ratings"

GAMES_PATH      = "data/games_with_distance.csv"
RATINGS_PATH    = "data/barttorvik_ratings.csv"
# ───────────────────────────────────────────────────────────────────────────────


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
    Joins Barttorvik ratings onto games for both the winner and loser.
    Ratings are matched by (team, season).
    """
    rating_cols = [
        "team", "season", "rank", "adjoe", "adjde", "barthag",
        "adj_tempo", "wab", "efg_pct", "efg_d_pct", "tor", "tord",
        "orb", "drb", "ftr", "ftrd"
    ]
    rat = ratings[rating_cols].copy()

    # Join for winner
    games = games.merge(
        rat.rename(columns={c: f"winner_{c}" for c in rating_cols if c not in ("team","season")}),
        how="left",
        left_on=["winner", "season"],
        right_on=["team", "season"]
    ).drop(columns=["team"])

    # Join for loser
    games = games.merge(
        rat.rename(columns={c: f"loser_{c}" for c in rating_cols if c not in ("team","season")}),
        how="left",
        left_on=["loser", "season"],
        right_on=["team", "season"],
        suffixes=("", "_loser_dup")
    ).drop(columns=["team"])

    # Derived features useful for ML
    games["adjoe_diff"]     = games["winner_adjoe"]  - games["loser_adjoe"]
    games["adjde_diff"]     = games["winner_adjde"]  - games["loser_adjde"]
    games["barthag_diff"]   = games["winner_barthag"] - games["loser_barthag"]
    games["tempo_diff"]     = games["winner_adj_tempo"] - games["loser_adj_tempo"]
    games["dist_diff_miles"] = games["winner_dist_miles"] - games["loser_dist_miles"]

    return games


def build_game_document(row: dict) -> dict:
    """
    Converts a merged DataFrame row into a structured MongoDB document.
    """
    return {
        "season":       row.get("season"),
        "date":         row.get("date"),
        "neutral_site": True,
        "winner": {
            "team":         row.get("winner"),
            "pts":          row.get("winner_pts"),
            "home_lat":     row.get("winner_home_lat"),
            "home_lon":     row.get("winner_home_lon"),
            "dist_miles":   row.get("winner_dist_miles"),
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
            }
        },
        "loser": {
            "team":         row.get("loser"),
            "pts":          row.get("loser_pts"),
            "home_lat":     row.get("loser_home_lat"),
            "home_lon":     row.get("loser_home_lon"),
            "dist_miles":   row.get("loser_dist_miles"),
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
            }
        },
        "venue": {
            "lat": row.get("venue_lat"),
            "lon": row.get("venue_lon"),
        },
        "derived": {
            "point_diff":      row.get("point_diff"),
            "dist_diff_miles": row.get("dist_diff_miles"),   # winner_dist - loser_dist
            "adjoe_diff":      row.get("adjoe_diff"),        # winner_adjoe - loser_adjoe
            "adjde_diff":      row.get("adjde_diff"),
            "barthag_diff":    row.get("barthag_diff"),
            "tempo_diff":      row.get("tempo_diff"),
        },
        "ingested_at": datetime.utcnow()
    }


def ingest_games(collection, df: pd.DataFrame):
    """Upserts game documents into MongoDB."""
    ops = []
    for _, row in df.iterrows():
        doc = build_game_document(row.to_dict())
        doc = clean_nan(doc)
        # Use season + winner + loser + date as natural key
        filter_key = {
            "season": doc["season"],
            "date":   doc["date"],
            "winner.team": doc["winner"]["team"],
            "loser.team":  doc["loser"]["team"],
        }
        ops.append(UpdateOne(filter_key, {"$set": doc}, upsert=True))

    if ops:
        result = collection.bulk_write(ops)
        print(f"  Games upserted: {result.upserted_count} new, {result.modified_count} updated")


def ingest_ratings(collection, df: pd.DataFrame):
    """Upserts team rating documents into MongoDB."""
    ops = []
    for _, row in df.iterrows():
        doc = clean_nan(row.to_dict())
        doc["ingested_at"] = datetime.utcnow()
        filter_key = {"team": doc["team"], "season": doc["season"]}
        ops.append(UpdateOne(filter_key, {"$set": doc}, upsert=True))

    if ops:
        result = collection.bulk_write(ops)
        print(f"  Ratings upserted: {result.upserted_count} new, {result.modified_count} updated")


def main():
    # Load data
    print("Loading CSVs...")
    games   = pd.read_csv(GAMES_PATH)
    ratings = pd.read_csv(RATINGS_PATH)
    print(f"  Games loaded:   {len(games)}")
    print(f"  Ratings loaded: {len(ratings)}")

    # Merge
    print("\nMerging ratings onto games...")
    merged = merge_ratings(games, ratings)
    print(f"  Merged shape: {merged.shape}")

    # Ingest into MongoDB
    print("\nConnecting to MongoDB Atlas...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    print(f"Ingesting games into '{DB_NAME}.{GAMES_COL}'...")
    ingest_games(db[GAMES_COL], merged)

    print(f"Ingesting ratings into '{DB_NAME}.{RATINGS_COL}'...")
    ingest_ratings(db[RATINGS_COL], ratings)

    # Create useful indexes
    print("\nCreating indexes...")
    db[GAMES_COL].create_index([("season", 1), ("winner.team", 1)])
    db[GAMES_COL].create_index([("season", 1), ("loser.team", 1)])
    db[GAMES_COL].create_index([("derived.dist_diff_miles", 1)])
    db[RATINGS_COL].create_index([("team", 1), ("season", 1)], unique=True)
    print("  Indexes created.")

    # Summary
    game_count   = db[GAMES_COL].count_documents({})
    rating_count = db[RATINGS_COL].count_documents({})
    print(f"\nDatabase '{DB_NAME}' summary:")
    print(f"  {GAMES_COL}:    {game_count} documents")
    print(f"  {RATINGS_COL}: {rating_count} documents")

    client.close()
    print("\nDone!")


if __name__ == "__main__":
    main()

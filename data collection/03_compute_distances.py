"""
03_compute_distances.py
-----------------------
Geocodes each team's home campus and computes:
  - Distance (miles) from each team's home to the West Region venue
  - Longitude difference (team_lon - opponent_lon): positive = team is further west
  - A "westness" score: how much further west the winner was vs the loser

For the West Region hypothesis, longitude difference is the key variable:
a team with a more negative longitude (further west) playing in a western
venue should have a geographic/cultural familiarity advantage.

Inputs:
    data/raw_games.csv            (from 01_scrape_games.py)
Outputs:
    data/school_locations.json    (geocode cache)
    data/games_with_distance.csv

Dependencies: pip install geopy pandas numpy
"""

import pandas as pd
import numpy as np
import time
import os
import json
import logging
from pathlib import Path
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

# ── Configuration ──────────────────────────────────────────────────────────────
GAMES_PATH      = "data/raw_games.csv"
LOCATIONS_CACHE = "data/school_locations.json"
OUTPUT_GAMES    = "data/games_with_distance.csv"
SLEEP_SECONDS   = 1.1    # Nominatim requires >=1s between requests

# ── Logging ────────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/03_compute_distances.log", mode="w"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("compute_distances")

# ── Manual school location overrides ──────────────────────────────────────────
# Geocoding by school name is unreliable; these are exact lat/lon for campus.
SCHOOL_LOCATIONS = {
    "Gonzaga":            (47.6673, -117.4022),
    "Arizona":            (32.2319, -110.9501),
    "Arizona State":      (33.4242, -111.9281),
    "UCLA":               (34.0689, -118.4452),
    "USC":                (34.0224, -118.2851),
    "Oregon":             (44.0461, -123.0722),
    "Oregon State":       (44.5646, -123.2620),
    "Washington":         (47.6553, -122.3035),
    "Washington State":   (46.7298, -117.1817),
    "Utah":               (40.7649, -111.8421),
    "Utah State":         (41.7458, -111.8135),
    "Colorado":           (40.0076, -105.2659),
    "Colorado State":     (40.5734, -105.0865),
    "Nevada":             (39.5470, -119.8150),
    "UNLV":               (36.1076, -115.1425),
    "San Diego State":    (32.7757, -117.0719),
    "BYU":                (40.2338, -111.6585),
    "New Mexico":         (35.0844, -106.6504),
    "Boise State":        (43.6019, -116.1995),
    "Fresno State":       (36.8133, -119.7468),
    "Kansas":             (38.9543, -95.2558),
    "Kansas State":       (39.1836, -96.5717),
    "Kentucky":           (38.0306, -84.5037),
    "Duke":               (36.0014, -78.9382),
    "North Carolina":     (35.9049, -79.0469),
    "UNC":                (35.9049, -79.0469),
    "Michigan":           (42.2780, -83.7382),
    "Michigan State":     (42.7251, -84.4791),
    "Villanova":          (40.0358, -75.3435),
    "Texas":              (30.2849, -97.7341),
    "Texas Tech":         (33.5843, -101.8783),
    "Baylor":             (31.5489, -97.1131),
    "Houston":            (29.7199, -95.3422),
    "Florida":            (29.6465, -82.3533),
    "Florida State":      (30.4418, -84.2985),
    "Alabama":            (33.2098, -87.5692),
    "Auburn":             (32.6099, -85.4808),
    "Tennessee":          (35.9546, -83.9296),
    "Purdue":             (40.4259, -86.9081),
    "Indiana":            (39.1682, -86.5230),
    "Ohio State":         (40.0061, -83.0282),
    "Iowa":               (41.6611, -91.5302),
    "Iowa State":         (42.0266, -93.6465),
    "Illinois":           (40.1020, -88.2272),
    "Wisconsin":          (43.0753, -89.4081),
    "Minnesota":          (44.9740, -93.2277),
    "Xavier":             (39.1479, -84.4726),
    "Creighton":          (41.2565, -96.0090),
    "Marquette":          (43.0389, -87.9365),
    "Providence":         (41.8376, -71.4422),
    "UConn":              (41.8079, -72.2550),
    "Connecticut":        (41.8079, -72.2550),
    "Seton Hall":         (40.7452, -74.2435),
    "St. John's":         (40.7219, -73.7949),
    "Georgetown":         (38.9076, -77.0723),
    "Syracuse":           (43.0351, -76.1354),
    "Pittsburgh":         (40.4443, -79.9602),
    "Virginia":           (38.0336, -78.5080),
    "Virginia Tech":      (37.2284, -80.4234),
    "North Carolina State": (35.7872, -78.6872),
    "Wake Forest":        (36.1340, -80.2784),
    "Notre Dame":         (41.7052, -86.2353),
    "Louisville":         (38.2253, -85.7585),
    "Oklahoma":           (35.2059, -97.4455),
    "Oklahoma State":     (36.1269, -97.0682),
    "TCU":                (32.7096, -97.3639),
    "SMU":                (32.8412, -96.7840),
    "Arkansas":           (36.0681, -94.1737),
    "LSU":                (30.4133, -91.1800),
    "Mississippi State":  (33.4554, -88.7923),
    "Ole Miss":           (34.3654, -89.5379),
    "Georgia":            (33.9480, -83.3771),
    "South Carolina":     (33.9963, -81.0210),
    "Missouri":           (38.9404, -92.3277),
    "Nebraska":           (40.8202, -96.7005),
    "North Dakota":       (46.9199, -96.7981),
    "North Dakota State": (46.8972, -96.8022),
    "South Dakota State": (44.3190, -96.7898),
    "Montana State":      (45.6669, -111.0541),
    "Eastern Washington": (47.6010, -117.5644),
    "Norfolk State":      (36.8468, -76.2863),
    "Howard":             (38.9218, -77.0200),
    "Texas Southern":     (29.7270, -95.3599),
    "Alcorn State":       (31.9282, -90.9726),
    "College of Charleston": (32.7835, -79.9374),
    "Charleston":         (32.7835, -79.9374),
    "Clemson":            (34.6834, -82.8374),
    "Wichita State":      (37.7197, -97.2948),
    "Saint Mary's (CA)":  (37.8494, -122.1189),
    "Pacific":            (37.9838, -121.3153),
    "Long Beach State":   (33.7838, -118.1141),
    "UC Santa Barbara":   (34.4140, -119.8489),
    "UC Irvine":          (33.6405, -117.8443),
    "UC Davis":           (38.5382, -121.7617),
    "Cal":                (37.8724, -122.2595),
    "Stanford":           (37.4275, -122.1697),
    "VCU":                (37.5477, -77.4526),
    "Memphis":            (35.1495, -90.0490),
    "Cincinnati":         (39.1329, -84.5150),
}


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute great-circle distance in miles between two lat/lon points."""
    R = 3958.8
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlam = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlam/2)**2
    return R * 2 * np.arcsin(np.sqrt(a))


def load_cache(path: str) -> dict:
    """Load geocode cache from JSON file."""
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {}


def save_cache(cache: dict, path: str) -> None:
    """Save geocode cache to JSON file."""
    with open(path, "w") as f:
        json.dump(cache, f, indent=2)


def get_location(name: str, geolocator, cache: dict) -> tuple:
    """
    Return (lat, lon) for a school name.

    Checks manual overrides first, then cache, then geocodes via Nominatim.
    Saves result to cache after each lookup to avoid re-geocoding on restart.

    Parameters
    ----------
    name       : School name string.
    geolocator : Nominatim geolocator instance.
    cache      : Mutable dict used as persistent geocode cache.

    Returns
    -------
    tuple : (lat, lon) or (None, None) on failure.
    """
    # 1. Manual override — most reliable
    if name in SCHOOL_LOCATIONS:
        return SCHOOL_LOCATIONS[name]

    # 2. Cache hit
    if name in cache:
        return tuple(cache[name]) if cache[name] else (None, None)

    # 3. Geocode via Nominatim
    query = f"{name} university"
    try:
        time.sleep(SLEEP_SECONDS)
        loc = geolocator.geocode(query, timeout=10)
        if loc:
            cache[name] = [loc.latitude, loc.longitude]
            log.info(f"  Geocoded: {name!r} → ({loc.latitude:.4f}, {loc.longitude:.4f})")
            return loc.latitude, loc.longitude
        else:
            log.warning(f"  Could not geocode: {name!r}")
            cache[name] = None
            return None, None
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        log.warning(f"  Geocoder error for {name!r}: {e}")
        cache[name] = None
        return None, None


def main():
    """
    Main entry point. Geocodes all teams, computes distances and
    longitude differences, writes enriched games CSV.
    """
    os.makedirs("data", exist_ok=True)

    games = pd.read_csv(GAMES_PATH)
    log.info(f"Loaded {len(games)} games from {GAMES_PATH}")

    cache = load_cache(LOCATIONS_CACHE)
    geolocator = Nominatim(user_agent="ncaab_west_region_ds4320")

    all_teams = set(games["winner"].dropna()) | set(games["loser"].dropna())
    log.info(f"Unique teams to locate: {len(all_teams)}")

    # Pre-warm cache for all teams
    for team in sorted(all_teams):
        if team not in SCHOOL_LOCATIONS and team not in cache:
            get_location(team, geolocator, cache)
            save_cache(cache, LOCATIONS_CACHE)

    save_cache(cache, LOCATIONS_CACHE)
    log.info("Location lookup complete.")

    # ── Compute per-game distance and longitude features ──────────────────
    results = []
    for _, row in games.iterrows():
        winner, loser = row["winner"], row["loser"]

        w_lat, w_lon = get_location(winner, geolocator, cache)
        l_lat, l_lon = get_location(loser,  geolocator, cache)

        # Venue lat/lon already set in script 01 from known tournament sites
        v_lat = row.get("venue_lat")
        v_lon = row.get("venue_lon")

        if all(x is not None for x in [w_lat, w_lon, l_lat, l_lon, v_lat, v_lon]):
            winner_dist = haversine_miles(w_lat, w_lon, v_lat, v_lon)
            loser_dist  = haversine_miles(l_lat, l_lon, v_lat, v_lon)

            # Longitude difference: negative = further west.
            # winner_lon_diff > 0 means winner is east of loser (less western).
            # For the hypothesis we want: does lower (more negative) winner_lon
            # correlate with winning?
            winner_lon_diff = w_lon - l_lon   # negative = winner further west
            venue_lon_diff  = w_lon - float(v_lon)  # winner lon vs venue lon
        else:
            winner_dist = loser_dist = winner_lon_diff = venue_lon_diff = None

        results.append({
            **row.to_dict(),
            "winner_lat":       w_lat,
            "winner_lon":       w_lon,
            "loser_lat":        l_lat,
            "loser_lon":        l_lon,
            "winner_dist_miles": winner_dist,
            "loser_dist_miles":  loser_dist,
            "dist_diff_miles":   (winner_dist - loser_dist) if winner_dist is not None else None,
            # Key hypothesis variable: negative = winner was further west than loser
            "winner_lon_diff":   winner_lon_diff,
            # How far west winner is relative to venue
            "winner_venue_lon_diff": venue_lon_diff,
        })

    df_out = pd.DataFrame(results)
    df_out.to_csv(OUTPUT_GAMES, index=False)

    missing = df_out["winner_dist_miles"].isna().sum()
    log.info(f"Saved {len(df_out)} games to {OUTPUT_GAMES}")
    log.info(f"Missing distance data: {missing} ({100*missing/len(df_out):.1f}%)")
    print(df_out[["winner", "loser", "winner_lon", "loser_lon",
                  "winner_lon_diff", "winner_dist_miles", "loser_dist_miles"]].head(10).to_string())


if __name__ == "__main__":
    main()

"""
03_compute_distances.py
-----------------------
Geocodes each NCAAB school's home arena using geopy (OpenStreetMap/Nominatim)
and computes haversine distance (miles) from each team's home to the
neutral-site game venue.

Inputs:
  data/raw_games.csv          (from 01_scrape_games.py)

Outputs:
  data/school_locations.csv   (team → lat/lon cache)
  data/games_with_distance.csv

Dependencies: pip install geopy pandas numpy
"""

import pandas as pd
import numpy as np
import time
import os
import json
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

# ── Configuration ──────────────────────────────────────────────────────────────
GAMES_PATH      = "data/raw_games.csv"
LOCATIONS_CACHE = "data/school_locations.json"   # cache to avoid re-geocoding
OUTPUT_GAMES    = "data/games_with_distance.csv"
SLEEP_SECONDS   = 1.1   # Nominatim requires ≥1s between requests
# ───────────────────────────────────────────────────────────────────────────────

# Known venue locations for major neutral-site tournaments.
# Add more as you encounter them — this saves geocoding failures on venue names.
KNOWN_VENUES = {
    "NCAA Tournament": None,   # will be geocoded from game city if available
    "Madison Square Garden": (40.7505, -73.9934),
    "United Center": (41.8807, -87.6742),
    "Barclays Center": (40.6826, -73.9754),
    "T-Mobile Arena": (36.1028, -115.1784),
    "Spectrum Center": (35.2251, -80.8392),
    "Gainbridge Fieldhouse": (39.7639, -86.1555),
    "Fiserv Forum": (43.0450, -87.9170),
    "Toyota Center": (29.7508, -95.3621),
    "Ball Arena": (39.7487, -105.0077),
    "Capital One Arena": (38.8981, -77.0209),
    "PPG Paints Arena": (40.4396, -79.9892),
    "Paycom Center": (35.4634, -97.5151),
    "Crypto.com Arena": (34.0430, -118.2673),
    "Kaseya Center": (25.7814, -80.1870),
}

# Manual overrides for school names that geopy struggles with.
# Key = name as it appears in Sports-Reference, Value = search string for geopy.
SCHOOL_GEOCODE_OVERRIDES = {
    "UConn":               "University of Connecticut, Storrs, CT",
    "UNC":                 "University of North Carolina, Chapel Hill, NC",
    "NC State":            "NC State University, Raleigh, NC",
    "LSU":                 "Louisiana State University, Baton Rouge, LA",
    "USC":                 "University of Southern California, Los Angeles, CA",
    "UCF":                 "University of Central Florida, Orlando, FL",
    "UNLV":                "University of Nevada Las Vegas, Las Vegas, NV",
    "VCU":                 "Virginia Commonwealth University, Richmond, VA",
    "SMU":                 "Southern Methodist University, Dallas, TX",
    "TCU":                 "Texas Christian University, Fort Worth, TX",
    "BYU":                 "Brigham Young University, Provo, UT",
    "UAB":                 "University of Alabama Birmingham, Birmingham, AL",
    "UTEP":                "University of Texas El Paso, El Paso, TX",
    "UTSA":                "University of Texas San Antonio, San Antonio, TX",
    "FIU":                 "Florida International University, Miami, FL",
    "FAU":                 "Florida Atlantic University, Boca Raton, FL",
    "Ole Miss":            "University of Mississippi, Oxford, MS",
    "Mississippi State":   "Mississippi State University, Starkville, MS",
    "Miami (FL)":          "University of Miami, Coral Gables, FL",
    "Miami (OH)":          "Miami University, Oxford, OH",
    "Saint Mary's (CA)":   "Saint Mary's College, Moraga, CA",
    "Loyola Chicago":      "Loyola University Chicago, Chicago, IL",
    "Loyola (MD)":         "Loyola University Maryland, Baltimore, MD",
    "Illinois State":      "Illinois State University, Normal, IL",
    "Indiana State":       "Indiana State University, Terre Haute, IN",
    "Iowa State":          "Iowa State University, Ames, IA",
    "Kansas State":        "Kansas State University, Manhattan, KS",
    "Michigan State":      "Michigan State University, East Lansing, MI",
    "Ohio State":          "Ohio State University, Columbus, OH",
    "Penn State":          "Penn State University, University Park, PA",
    "Arizona State":       "Arizona State University, Tempe, AZ",
    "Florida State":       "Florida State University, Tallahassee, FL",
    "Colorado State":      "Colorado State University, Fort Collins, CO",
    "Oregon State":        "Oregon State University, Corvallis, OR",
    "Washington State":    "Washington State University, Pullman, WA",
}


def haversine_miles(lat1, lon1, lat2, lon2):
    """Compute great-circle distance in miles between two lat/lon points."""
    R = 3958.8  # Earth radius in miles
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi  = np.radians(lat2 - lat1)
    dlam  = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlam/2)**2
    return R * 2 * np.arcsin(np.sqrt(a))


def load_location_cache(cache_path):
    if os.path.exists(cache_path):
        with open(cache_path, "r") as f:
            return json.load(f)
    return {}


def save_location_cache(cache, cache_path):
    with open(cache_path, "w") as f:
        json.dump(cache, f, indent=2)


def geocode_entity(name: str, geolocator, cache: dict):
    """
    Looks up lat/lon for a school or venue name.
    Uses cache first, then Nominatim, with manual overrides.
    Returns (lat, lon) tuple or (None, None) on failure.
    """
    if name in cache:
        return tuple(cache[name]) if cache[name] else (None, None)

    # Check known venues
    if name in KNOWN_VENUES and KNOWN_VENUES[name] is not None:
        cache[name] = list(KNOWN_VENUES[name])
        return KNOWN_VENUES[name]

    # Apply school name overrides
    query = SCHOOL_GEOCODE_OVERRIDES.get(name, f"{name} university arena")

    try:
        time.sleep(SLEEP_SECONDS)
        location = geolocator.geocode(query, timeout=10)
        if location:
            cache[name] = [location.latitude, location.longitude]
            print(f"    Geocoded: {name!r} → ({location.latitude:.4f}, {location.longitude:.4f})")
            return location.latitude, location.longitude
        else:
            # Try simpler query
            time.sleep(SLEEP_SECONDS)
            location = geolocator.geocode(name, timeout=10)
            if location:
                cache[name] = [location.latitude, location.longitude]
                print(f"    Geocoded (fallback): {name!r} → ({location.latitude:.4f}, {location.longitude:.4f})")
                return location.latitude, location.longitude
            else:
                print(f"    WARNING: Could not geocode {name!r}")
                cache[name] = None
                return None, None
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        print(f"    Geocoder error for {name!r}: {e}")
        cache[name] = None
        return None, None


def main():
    os.makedirs("data", exist_ok=True)

    # Load game data
    games = pd.read_csv(GAMES_PATH)
    print(f"Loaded {len(games)} neutral-site games from {GAMES_PATH}")

    # Load cache
    cache = load_location_cache(LOCATIONS_CACHE)

    # Collect unique school names to geocode
    all_teams = set(games["winner"].dropna()) | set(games["loser"].dropna())
    print(f"Unique teams to geocode: {len(all_teams)}")

    geolocator = Nominatim(user_agent="ncaab_distance_project_ds4320")

    # Geocode all teams
    print("\nGeocoding team home arenas...")
    for team in sorted(all_teams):
        if team not in cache:
            geocode_entity(team, geolocator, cache)
            save_location_cache(cache, LOCATIONS_CACHE)  # save after each to avoid losing progress

    save_location_cache(cache, LOCATIONS_CACHE)
    print(f"\nGeocoding complete. Cache saved to {LOCATIONS_CACHE}")

    # ── Compute distances ──────────────────────────────────────────────────────
    # For neutral-site games we approximate the venue as the geographic midpoint
    # between the two teams' home locations. This is a reasonable proxy when
    # the actual venue city is not available in the scraped data.
    # If you have venue city data, replace midpoint logic with venue geocoding.

    print("\nComputing distances...")
    results = []

    for _, row in games.iterrows():
        winner, loser = row["winner"], row["loser"]

        w_lat, w_lon = geocode_entity(winner, geolocator, cache) if winner not in cache else (
            (cache[winner][0], cache[winner][1]) if cache[winner] else (None, None)
        )
        l_lat, l_lon = geocode_entity(loser, geolocator, cache) if loser not in cache else (
            (cache[loser][0], cache[loser][1]) if cache[loser] else (None, None)
        )

        # Estimate venue as midpoint (replace with actual venue coords if available)
        if all(x is not None for x in [w_lat, w_lon, l_lat, l_lon]):
            venue_lat = (w_lat + l_lat) / 2
            venue_lon = (w_lon + l_lon) / 2
            winner_dist = haversine_miles(w_lat, w_lon, venue_lat, venue_lon)
            loser_dist  = haversine_miles(l_lat, l_lon, venue_lat, venue_lon)
            dist_diff   = winner_dist - loser_dist
        else:
            venue_lat = venue_lon = winner_dist = loser_dist = dist_diff = None

        results.append({
            **row.to_dict(),
            "winner_home_lat":  w_lat,
            "winner_home_lon":  w_lon,
            "loser_home_lat":   l_lat,
            "loser_home_lon":   l_lon,
            "venue_lat":        venue_lat,
            "venue_lon":        venue_lon,
            "winner_dist_miles": winner_dist,
            "loser_dist_miles":  loser_dist,
            "dist_diff_miles":   dist_diff,   # positive = winner traveled farther
        })

    df_out = pd.DataFrame(results)
    df_out.to_csv(OUTPUT_GAMES, index=False)
    missing = df_out["winner_dist_miles"].isna().sum()
    print(f"\nSaved {len(df_out)} games to {OUTPUT_GAMES}")
    print(f"Games missing distance data: {missing} ({100*missing/len(df_out):.1f}%)")
    print(df_out[["winner","loser","winner_dist_miles","loser_dist_miles","dist_diff_miles"]].head(10))


if __name__ == "__main__":
    main()

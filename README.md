# DS 4320 Project 2: Do Western Teams Have a Geographic Advantage in the NCAA West Region?

**Executive Summary:** This repository contains a fully established secondary dataset and analysis pipeline built to test whether NCAA Tournament teams located further west than their opponent perform better in West Region games. Game results from the 2015–2025 NCAA Tournament West Region were manually collected from Sports-Reference, team-season efficiency ratings were sourced from the Kaggle college basketball dataset (Barttorvik-derived T-Rank metrics), and travel distances from each team's home campus to the known West Region venue were computed using geocoding and the haversine formula. The combined dataset was ingested into a MongoDB Atlas document database (`ncaab_west_region`). An analysis pipeline (Jupyter notebook) queries this database, engineers features centered on longitude difference as the key hypothesis variable, and applies machine learning classification to test whether geographic westerness predicts game outcomes after controlling for team quality and seeding.

**Name:** Kieran Perdue

**NetID:** rrx5eg

**DOI:** [10.5281/zenodo.19898678](https://doi.org/10.5281/zenodo.19898678)

**Press Release:** [press_release.md](Press_Release.md)

**Pipeline:** [05_pipeline.ipynb](pipeline/05_pipeline.md)

**License:** MIT — [LICENSE](LICENSE)

---

## Problem Definition

**General Problem:** Predicting sports game outcomes (General Problem #7).

**Specific Problem:** Do NCAA Tournament teams located significantly further west than their opponent win more often — and by larger margins — in West Region games, after controlling for team quality (Barttorvik efficiency ratings) and seeding?

The NCAA Tournament is one of the most-watched sporting events in the United States, yet bracket prediction remains notoriously difficult. The bulk of analytical work focuses on team quality metrics such as adjusted efficiency and power ratings. Geographic factors — specifically whether playing closer to a team's home region conveys any advantage at a neutral site — are far less studied. The West Region of the NCAA Tournament is particularly interesting because its venues (Sacramento, San Diego, San Jose, Los Angeles, Las Vegas, San Francisco, Salt Lake City) are clustered in the western United States, which may systematically favor programs from that region through reduced travel, larger fan bases in attendance, and reduced time zone disruption.

The refinement from the general problem of "predicting sports game outcomes" to this specific geographic hypothesis is driven by a gap in the existing literature. While home-court advantage is extensively documented, the question of whether regional proximity to a neutral-site venue conveys a measurable benefit — independent of team quality — has received little rigorous attention at the college level. The West Region provides a clean natural experiment: a fixed, recurring set of games at consistently western venues over a decade, with well-documented team ratings available for controls. If a significant longitude effect exists, it has practical implications for bracket strategy, NCAA seeding policy, and the academic literature on competitive balance in college athletics.

**Press Release Headline:** [Home Is Where the West Is: Do NCAA Tournament Teams Perform Better on Their Own Turf?](press_release.md)

---

## Domain Exposition

### Terminology

| Term | Definition |
|------|-----------|
| Neutral Site | A game venue that is not the home arena of either participating team |
| West Region | One of four geographic regions in the NCAA Tournament bracket; venues are historically in the western United States |
| AdjOE (Adjusted Offensive Efficiency) | Points scored per 100 possessions, adjusted for opponent strength |
| AdjDE (Adjusted Defensive Efficiency) | Points allowed per 100 possessions, adjusted for opponent strength |
| Barthag | Barttorvik power rating; estimated probability of beating an average D1 team on a neutral court |
| Longitude Difference | Winner's home longitude minus loser's home longitude; negative = winner is further west |
| Haversine Distance | Great-circle distance between two geographic coordinates, accounting for Earth's curvature |
| Seed | Tournament seeding 1–16; lower seed = higher-ranked team |
| WAB (Wins Above Bubble) | Wins above what a bubble NCAA Tournament team would be expected to accumulate given the same schedule |
| eFG% (Effective Field Goal Percentage) | FG% adjusted for the extra value of three-point shots: (FGM + 0.5 × 3PM) / FGA |
| T-Rank | Barttorvik's national team ranking system based on adjusted efficiency metrics |
| KenPom | A competing advanced analytics system for college basketball; methodology closely parallels Barttorvik |
| Point Differential | Winner's score minus loser's score; used as a continuous measure of game dominance |
| Time Zone Adjustment | The physiological and logistical burden of competing across multiple time zones |
| D1 | NCAA Division I, the highest level of collegiate athletics |

### Domain Background

This project sits at the intersection of sports analytics, geography, and data engineering. College basketball analytics has matured significantly since Ken Pomeroy popularized possession-adjusted efficiency metrics in the early 2000s. The core insight is that raw scoring statistics are heavily influenced by pace of play, so normalizing by possessions enables meaningful cross-team comparisons. Barttorvik's T-Rank system — the source of team ratings in this project — builds on this tradition with ridge-regression-based adjustments for opponent strength. The geographic dimension of this project draws on a smaller but growing literature on travel effects in professional sports, where studies have found that westward travel tends to be more fatiguing than eastward travel due to circadian rhythm disruption. Whether this effect is detectable in college basketball, where travel budgets and recovery infrastructure vary dramatically, is an open empirical question that this project addresses directly.

### Background Reading

> Upload copies of the articles below to a shared OneDrive folder and update the links. One point per article, up to 5 points.

| Title | Description | Link |
|-------|-------------|------|
| Barttorvik T-Rank Methodology | Overview of how T-Rank efficiency ratings are computed and adjusted for opponent strength | [barttorvik.com](https://barttorvik.com) |
| Kaggle College Basketball Dataset | Documentation for the cbb.csv dataset used as the source of team efficiency ratings | [kaggle.com/datasets/andrewsundberg/college-basketball-dataset](https://www.kaggle.com/datasets/andrewsundberg/college-basketball-dataset) |
| *[Add article on travel fatigue in sports]* | Peer-reviewed piece on how travel distance and time zones affect athletic performance | [link to OneDrive copy] |
| *[Add article on NCAA Tournament prediction]* | Overview of machine learning approaches to bracket prediction | [link to OneDrive copy] |
| *[Add article on home court / regional advantage]* | Quantification of regional familiarity effects at neutral-site sporting events | [link to OneDrive copy] |

---

## Data Creation

### Provenance

Raw game data was manually exported from Sports-Reference's CBB Play Index (sports-reference.com/cbb) via the browser interface — automated scraping was not feasible due to the site's bot-detection systems returning 403/404 responses for all programmatic requests. The exported CSV (`data/West_results.csv`) contains all NCAA Tournament West Region game results from 2015–2025, with each game appearing twice (once per team). Script `01_scrape_games.py` processes this file: it deduplicates by retaining only the winning team's row, standardizes team names to match Barttorvik conventions, and attaches known West Region venue coordinates from a hard-coded lookup table sourced from NCAA public tournament records.

Team efficiency ratings were sourced from the Kaggle college basketball dataset (`data/cbb.csv`), which compiles Barttorvik T-Rank metrics for all D1 teams from 2013–2023. Script `02_pull_barttorvik.py` loads this file and standardizes column names to match pipeline conventions. Travel distances were computed in `03_compute_distances.py` by geocoding each school's home campus location — using a manually curated lookup table of 80+ programs for accuracy, with Nominatim as a fallback for unlisted schools — and then applying the haversine formula to calculate great-circle distance in miles from each team's home to the actual known West Region venue city for that year. The final merge and MongoDB ingestion is performed by `04_merge_and_ingest.py`, which joins game records with efficiency ratings on `(team, season)` and constructs nested JSON documents containing game metadata, per-team ratings, venue coordinates, and derived hypothesis variables.

### Code

| File | Description | Link |
|------|-------------|------|
| `01_scrape_games.py` | Processes the manually downloaded West Region CSV, deduplicates, standardizes team names, and attaches known venue coordinates. Outputs `data/raw_games.csv`. | [01_scrape_games.py](01_scrape_games.py) |
| `02_pull_barttorvik.py` | Loads Barttorvik T-Rank metrics from the Kaggle cbb.csv dataset and standardizes column names. Outputs `data/barttorvik_ratings.csv`. | [02_pull_barttorvik.py](02_pull_barttorvik.py) |
| `03_compute_distances.py` | Geocodes school home locations and computes haversine distances to the actual West Region venue for each season. Outputs `data/games_with_distance.csv`. | [03_compute_distances.py](03_compute_distances.py) |
| `04_merge_and_ingest.py` | Merges game and rating data, builds nested MongoDB documents with hypothesis variables, and upserts into MongoDB Atlas (`ncaab_west_region`). | [04_merge_and_ingest.py](04_merge_and_ingest.py) |

### Critical Decisions and Uncertainty

The most significant improvement over a naive approach is the use of **actual known West Region venue cities** rather than a midpoint heuristic. Unlike generic neutral-site game databases, the NCAA Tournament's venue locations are public record, so every distance in this dataset is computed against the real arena city — Sacramento, San Diego, Las Vegas, Los Angeles, San Francisco, Salt Lake City, or San Jose depending on the year. This substantially reduces distance measurement error compared to midpoint approximation.

The most important judgment call is treating 2021 (the COVID bubble year) as a normal season. All 2021 games were played in Indianapolis regardless of regional assignment, which removes geographic variation for that year entirely. These games are retained in the dataset but noted as a known confounder — the 2021 season systematically suppresses the longitude signal. A sensitivity analysis excluding 2021 is recommended.

The Kaggle rating dataset ends at 2023, so 2024 and 2025 games have no Barttorvik ratings attached. These games contribute to the raw geographic analysis (longitude difference, point margin) but are excluded from the ML modeling sample, reducing the modeling sample from 133 to 76 games.

### Bias Identification

**Selection bias:** West Region games disproportionately feature Power 6 conference programs. The region historically includes many Pac-12/12-now-independent schools (Gonzaga, Arizona, UCLA) which are both geographically western and high-quality — creating confounding between quality and longitude that the model must disentangle. **Temporal bias:** Conference realignment (Pac-12 dissolution, 2023–2024) may affect which programs appear in the West Region in later seasons, altering the geographic distribution of teams. **Coverage bias:** The Kaggle ratings file ends at 2023, so the two most recent seasons are missing from the modeling sample. **Geocoding bias:** Distance estimates for schools in dense urban areas are more precise than for rural campuses, though the manual lookup table mitigates this for the most common programs.

### Bias Mitigation

The model includes `barthag_diff` and `seed_diff` as explicit controls for team quality, allowing `lon_diff` to be evaluated after accounting for the quality confound. Season fixed effects can be added to address temporal bias. The 2021 bubble year is flagged in the dataset and can be excluded in sensitivity analyses. Missing ratings (2024–2025) are stored as null values rather than dropped from the database, preserving game outcome data for geographic-only analyses.

---

## Metadata

### Implicit Schema — `games` Collection

Each document in `ncaab_west_region.games` represents one West Region NCAA Tournament game. The `hypothesis` subdocument contains all key variables for testing the geographic hypothesis.

```
{
  season:        <int>     // Ending year of the NCAA season (e.g. 2024 = 2023-24)
  date:          <string>  // Approximate game date (YYYY-MM-DD); exact date not in source export
  region:        <string>  // Always "West"
  neutral_site:  <bool>    // Always true
  venue: {
    city:        <string>  // Known West Region host city (e.g. "Las Vegas")
    lat:         <float>   // Venue latitude
    lon:         <float>   // Venue longitude
  },
  winner: {
    team:        <string>  // Team name (standardized to Barttorvik conventions)
    pts:         <float>   // Points scored
    seed:        <int>     // Tournament seed (1-16)
    lat:         <float>   // Home campus latitude
    lon:         <float>   // Home campus longitude
    dist_miles:  <float>   // Haversine distance from home to venue
    ratings: {
      rank:      <int>     // T-Rank national rank
      adjoe:     <float>   // Adjusted offensive efficiency
      adjde:     <float>   // Adjusted defensive efficiency
      barthag:   <float>   // Power rating (0-1)
      adj_tempo: <float>   // Adjusted possessions per 40 min
      wab:       <float>   // Wins above bubble
      efg_pct:   <float>   // Effective FG%
      efg_d_pct: <float>   // Effective FG% allowed
      tor:       <float>   // Turnover rate
      tord:      <float>   // Turnover rate forced
      orb:       <float>   // Offensive rebound rate
      drb:       <float>   // Defensive rebound rate
    }
  },
  loser: { ... },          // Identical structure to winner
  hypothesis: {
    lon_diff:        <float>  // winner_lon - loser_lon (negative = winner further west)
    dist_diff_miles: <float>  // winner_dist - loser_dist
    seed_diff:       <int>    // winner_seed - loser_seed
    barthag_diff:    <float>  // winner_barthag - loser_barthag
    adjoe_diff:      <float>  // winner_adjoe - loser_adjoe
    adjde_diff:      <float>  // winner_adjde - loser_adjde
  },
  ingested_at: <ISODate>      // UTC timestamp of document ingestion
}
```

### Implicit Schema — `team_ratings` Collection

Each document represents one team's season-level T-Rank statistics. One document per `(team, season)` pair.

```
{
  team:        <string>  // Team name
  season:      <int>     // Season ending year
  conf:        <string>  // Conference abbreviation
  rank:        <int>     // T-Rank national rank
  adjoe:       <float>
  adjde:       <float>
  barthag:     <float>
  efg_pct:     <float>
  efg_d_pct:   <float>
  tor:         <float>
  tord:        <float>
  orb:         <float>
  drb:         <float>
  adj_tempo:   <float>
  wab:         <float>
  ingested_at: <ISODate>
}
```

### Data Summary

| Collection | Documents | Seasons Covered | Key Join Field |
|---|---|---|---|
| `games` | 133 | 2015–2025 | `(season, winner.team, loser.team)` |
| `team_ratings` | 4,249 | 2013–2023 | `(team, season)` |

### Data Dictionary — `games` Collection

| Field | Type | Description | Example | Uncertainty |
|---|---|---|---|---|
| `season` | int | Ending calendar year of the NCAA season | `2024` | None — deterministic |
| `date` | string | Approximate game date | `"2024-03-01"` | Medium — exact date not in source export; month is correct |
| `winner.team` | string | Winning team name | `"Gonzaga"` | Low — direct from source; occasional name variant mismatches |
| `winner.pts` | float | Points scored by winner | `93.0` | Very low — official game scores |
| `winner.seed` | int | Tournament seed of winner | `1` | Very low — official NCAA bracket |
| `winner.lon` | float | Winner's home campus longitude | `-117.40` | Low — manually curated lookup table for major programs |
| `winner.dist_miles` | float | Distance from winner's home to venue | `412.3` | Low-medium — uses actual venue city; geocoding error ±10 miles |
| `winner.ratings.barthag` | float | Winner's power rating | `0.937` | Medium — season-level aggregate; does not reflect game-day form |
| `winner.ratings.adjoe` | float | Adjusted offensive efficiency | `118.4` | Medium — same caveat as barthag |
| `hypothesis.lon_diff` | float | winner_lon − loser_lon | `-14.2` | Low-medium — inherits geocoding precision from both teams |
| `hypothesis.dist_diff_miles` | float | winner_dist − loser_dist | `-226.4` | Low-medium — uses actual venue coordinates |
| `hypothesis.seed_diff` | int | winner_seed − loser_seed | `-3` | Very low — official NCAA bracket |
| `hypothesis.barthag_diff` | float | Quality gap between teams | `0.312` | Medium — propagated from both teams' rating uncertainty |
| `derived.point_diff` | float | Final score margin | `11.0` | Very low — official game scores |
| `venue.city` | string | Known West Region host city | `"Las Vegas"` | Very low — manually sourced from NCAA records |
| `ingested_at` | ISODate | UTC ingestion timestamp | `2026-04-29T...` | None — system-generated |

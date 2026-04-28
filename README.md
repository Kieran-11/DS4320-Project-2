# DS 4320 Project 2: Predicting NCAA Basketball Game Outcomes at Neutral Sites

**Executive Summary:** This repository contains a secondary dataset and analysis pipeline built to predict the outcome of NCAA Division I men's basketball games played at neutral sites. Game results from the 2015–2024 seasons were scraped from Sports-Reference, team-season efficiency ratings were pulled from Barttorvik (T-Rank), and travel distances from each team's home campus to the neutral venue were computed using geocoding and the haversine formula. The combined dataset was ingested into a MongoDB Atlas document database. An analysis pipeline (Jupyter notebook) then queries this database, engineers features, and applies machine learning classification to predict game winners — interrogating whether team quality metrics and travel burden explain outcomes at neutral venues where home-court advantage is absent.

**Name:** [YOUR NAME]  
**NetID:** [YOUR NETID]  
**DOI:** [YOUR ZENODO DOI — create at https://zenodo.org]  
**Press Release:** [press_release.md](press_release.md)  
**Pipeline:** [pipeline.ipynb](pipeline.ipynb)  
**License:** MIT — [LICENSE](LICENSE)

---

## Problem Definition

**General Problem:** Predicting sports game outcomes.

**Specific Problem:** Can the outcome of NCAA Division I men's basketball games played at neutral sites be predicted using pre-game team efficiency ratings and relative travel distance, and does travel asymmetry measurably influence win probability when neither team has a home-court advantage?

College basketball is one of the most analytically rich team sports in the United States, yet the bulk of predictive modeling focuses on games where one team has the structural advantage of playing at home. Neutral-site games — which occur frequently in early-season tournaments, conference tournaments, and the NCAA Tournament — remove this confound entirely, making them a cleaner environment in which to isolate the explanatory power of team quality and logistical factors. Travel fatigue is a well-documented phenomenon in professional sports but is understudied at the college level, where travel budgets and infrastructure vary dramatically across programs. A model that can reliably predict neutral-site outcomes has direct value for bracketology, sports betting markets, athletic program scheduling, and academic research on competitive balance.

The refinement from the general problem of "predicting sports game outcomes" to this specific focus on neutral-site NCAAB games is driven by methodological clarity. A general game-prediction model is complicated by home-court advantage, which is a large, noisy, and difficult-to-quantify effect. By restricting the dataset to neutral-site games only, that confounder is eliminated, allowing the model to focus on two cleaner questions: how much do efficiency differentials predict outcomes, and does the team that travels farther lose more often? This narrowing produces a more interpretable and academically interesting dataset while remaining practically useful for the millions of fans and analysts who follow college basketball tournaments.

**Press Release Headline:** [When Nobody Has Home Court: Can Travel Distance and Team Efficiency Predict the NCAA Tournament?](press_release.md)

---

## Domain Exposition

### Terminology

| Term | Definition |
|------|-----------|
| Neutral Site | A game venue that is not the home arena of either participating team |
| AdjOE (Adjusted Offensive Efficiency) | Points scored per 100 possessions, adjusted for opponent strength |
| AdjDE (Adjusted Defensive Efficiency) | Points allowed per 100 possessions, adjusted for opponent strength |
| Barthag | Barttorvik power rating; estimated probability of beating an average D1 team on a neutral court |
| Adj. Tempo | Adjusted possessions per 40 minutes; a measure of how fast a team plays |
| WAB (Wins Above Bubble) | Wins above what a bubble NCAA Tournament team would be expected to accumulate given the same schedule |
| eFG% (Effective Field Goal Percentage) | FG% adjusted to account for the extra value of three-point shots: (FGM + 0.5 × 3PM) / FGA |
| TO Rate | Turnovers per 100 possessions |
| OR% (Offensive Rebound Rate) | Percentage of available offensive rebounds secured |
| FT Rate | Free throw attempts per field goal attempt |
| Haversine Distance | Great-circle distance between two geographic coordinates, accounting for the curvature of the Earth |
| KenPom | A competing advanced analytics system for college basketball; methodology closely parallels Barttorvik |
| Bracketology | The practice of predicting the NCAA Tournament bracket and game outcomes |
| Point Differential | Winner's score minus loser's score; a common proxy for margin of victory |
| D1 | NCAA Division I, the highest level of collegiate athletics |

### Domain Background

This project exists at the intersection of sports analytics and data engineering. College basketball analytics has matured significantly since Ken Pomeroy popularized possession-adjusted efficiency metrics in the early 2000s. The core insight driving the field is that raw scoring statistics are heavily influenced by pace of play, so normalizing by possessions allows for meaningful cross-team comparisons. Barttorvik's T-Rank system, the data source for team ratings in this project, builds on this tradition with its own ridge-regression-based adjustments for opponent strength and venue. Neutral-site game prediction is of particular interest because the NCAA Tournament — the most-watched college sporting event in the United States — is played almost entirely at neutral sites, and accurate prediction models have obvious commercial and academic value.

### Background Reading

> **Note:** Upload copies of the articles below to a shared OneDrive folder and update the links. One point each, up to 5 points.

| Title | Description | Link |
|-------|-------------|------|
| Barttorvik T-Rank Methodology | Overview of how T-Rank efficiency ratings are computed and adjusted | [barttorvik.com](https://barttorvik.com) |
| Sports-Reference CBB Documentation | Explanation of data fields and methodology for Sports-Reference college basketball statistics | [sports-reference.com/cbb](https://www.sports-reference.com/cbb/) |
| *[Add article on travel fatigue in sports]* | Peer-reviewed or analytical piece on how travel distance affects team performance | [link to OneDrive copy] |
| *[Add article on NCAA Tournament prediction]* | Overview of machine learning approaches to bracket prediction | [link to OneDrive copy] |
| *[Add article on home court advantage in basketball]* | Quantification of home-court advantage and what happens when it is removed | [link to OneDrive copy] |

---

## Data Creation

### Provenance

Raw data for this project was collected from two publicly accessible sources. Game results were scraped from Sports-Reference (sports-reference.com/cbb), which compiles official NCAA statistics and historical schedules. The scraper (`01_scrape_games.py`) iterates over seasons 2015–2024, fetches the full season schedule page for each year, parses the HTML table using BeautifulSoup, and retains only rows where the location flag equals `"N"` (neutral site), yielding one row per neutral-site game with winner, loser, scores, and date. A 4-second courtesy delay between requests is enforced to comply with Sports-Reference's rate-limiting policies.

Team efficiency ratings were obtained from Barttorvik (barttorvik.com), which provides a free JSON API endpoint returning all D1 team ratings for a given season (`02_pull_barttorvik.py`). This endpoint requires no API key and returns 21 statistical columns per team per season including adjusted offensive and defensive efficiency, pace, rebound rates, turnover rates, and a composite power rating (Barthag). Travel distances were computed in `03_compute_distances.py` by geocoding each school's home location via OpenStreetMap's Nominatim API (via the `geopy` library) and then applying the haversine formula to calculate great-circle distance in miles from each team's home campus to the estimated venue location. Because the scraped game records do not contain venue city data, the venue coordinates are approximated as the geographic midpoint between the two teams' home locations — a known simplification discussed further below. The final merge and MongoDB ingestion is performed by `04_merge_and_ingest.py`, which joins game records with Barttorvik ratings on `(team, season)` and constructs nested JSON documents containing game metadata, per-team ratings, and derived differential features.

### Code

| File | Description | Link |
|------|-------------|------|
| `01_scrape_games.py` | Scrapes neutral-site NCAAB game results from Sports-Reference for seasons 2015–2024. Outputs `data/raw_games.csv`. | [01_scrape_games.py](01_scrape_games.py) |
| `02_pull_barttorvik.py` | Fetches T-Rank team efficiency ratings for all D1 teams from Barttorvik's JSON API. Outputs `data/barttorvik_ratings.csv`. | [02_pull_barttorvik.py](02_pull_barttorvik.py) |
| `03_compute_distances.py` | Geocodes school home locations via Nominatim and computes haversine travel distances to each neutral-site venue. Outputs `data/games_with_distance.csv` and a geocoding cache. | [03_compute_distances.py](03_compute_distances.py) |
| `04_merge_and_ingest.py` | Merges game and rating data, builds nested MongoDB documents, and upserts them into MongoDB Atlas. Also ingests the standalone team ratings collection. | [04_merge_and_ingest.py](04_merge_and_ingest.py) |

### Critical Decisions and Uncertainty

The most significant judgment call in this pipeline is the use of the geographic midpoint between the two teams' home campuses as a proxy for the neutral-site venue location. This approximation is necessary because the Sports-Reference schedule pages do not consistently include a parseable venue city field. The consequence is that individual distance estimates may be inaccurate, but the *relative* travel burden (who traveled farther, and by roughly how much) is directionally preserved for most games. The error is largest for games played very close to one team's home city — for example, a "neutral site" game in a team's home state — where the midpoint heuristic could assign near-zero distance to a team that actually traveled a moderate distance. A mitigation strategy would be to scrape venue city data from a secondary source (e.g., ESPN or NCAA.com) and geocode it directly; this is documented as a known limitation.

A secondary judgment call is the season date range (2015–2024). Seasons prior to 2015 have less complete Barttorvik data and the sport has changed significantly in style of play (the pace-and-space evolution), which would introduce non-stationarity. Limiting to 10 seasons balances data volume against consistency.

Name matching between Sports-Reference team names and Barttorvik team names introduces join failures for some programs. The `SCHOOL_GEOCODE_OVERRIDES` dictionary in `03_compute_distances.py` and the merge logic in `04_merge_and_ingest.py` address the most common mismatches, but some records will have missing rating data where team names could not be reconciled. These rows are retained in the database with null rating fields rather than dropped, preserving game outcome data for analyses that do not require efficiency ratings.

### Bias Identification

Several sources of bias may affect this dataset. **Selection bias** is inherent: neutral-site games disproportionately feature higher-ranked programs (Power 5 conferences, tournament participants), because smaller programs rarely travel to neutral-site tournaments. The dataset therefore over-represents elite programs and may produce models that generalize poorly to mid-major matchups. **Survivorship bias** exists in the Barttorvik ratings themselves, as teams that perform well generate more games and more data. **Geocoding bias** affects programs in dense urban areas differently from rural campuses — Nominatim is more reliable for flagship state universities than for smaller private institutions, introducing systematically missing distance data for some program types. **Temporal bias** may exist if conference realignments (e.g., programs moving between conferences 2015–2024) cause ratings to behave differently across the timeframe.

### Bias Mitigation

Tournament-context indicators and conference membership fields could be added to allow stratified analysis that separates high-major from mid-major matchups, partially addressing selection bias. The geocoding cache allows manual review and correction of failed lookups. For temporal bias, season fixed effects can be included in the model or separate models can be fit per season. Missing rating data is flagged with null values rather than imputed, ensuring downstream analyses can choose their own handling strategy transparently.

---

## Metadata

### Implicit Schema — `games` Collection

Each document in the `ncaab_neutral_site.games` collection represents a single neutral-site game and follows the structure below. All fields are optional in MongoDB's schema-free model, but the pipeline constructs every document with this shape.

```
{
  season:       <int>        // Ending year of the NCAA season (e.g. 2024 = 2023-24)
  date:         <string>     // Game date as ISO string (YYYY-MM-DD)
  neutral_site: <bool>       // Always true; included for filtering clarity
  winner: {
    team:       <string>     // Team name as it appears on Sports-Reference
    pts:        <float>      // Points scored
    home_lat:   <float>      // Home campus latitude (degrees)
    home_lon:   <float>      // Home campus longitude (degrees)
    dist_miles: <float>      // Haversine distance from home to estimated venue (miles)
    ratings: {
      rank:     <int>        // T-Rank national rank
      adjoe:    <float>      // Adjusted offensive efficiency
      adjde:    <float>      // Adjusted defensive efficiency
      barthag:  <float>      // Power rating (0–1)
      adj_tempo:<float>      // Adjusted possessions per 40 min
      wab:      <float>      // Wins above bubble
      efg_pct:  <float>      // Effective FG%
      efg_d_pct:<float>      // Effective FG% allowed
      tor:      <float>      // Turnover rate
      tord:     <float>      // Turnover rate forced
      orb:      <float>      // Offensive rebound rate
      drb:      <float>      // Defensive rebound rate
    }
  },
  loser: { ... }             // Identical structure to winner
  venue: {
    lat:        <float>      // Estimated venue latitude (midpoint heuristic)
    lon:        <float>      // Estimated venue longitude (midpoint heuristic)
  },
  derived: {
    point_diff:       <float>  // winner_pts - loser_pts
    dist_diff_miles:  <float>  // winner_dist - loser_dist (positive = winner traveled farther)
    adjoe_diff:       <float>  // winner_adjoe - loser_adjoe
    adjde_diff:       <float>  // winner_adjde - loser_adjde
    barthag_diff:     <float>  // winner_barthag - loser_barthag
    tempo_diff:       <float>  // winner_tempo - loser_tempo
  },
  ingested_at: <ISODate>       // UTC timestamp of document ingestion
}
```

### Implicit Schema — `team_ratings` Collection

Each document in `ncaab_neutral_site.team_ratings` represents one team's season-level T-Rank statistics. One document per `(team, season)` pair.

```
{
  team:           <string>  // Team name
  season:         <int>     // Season ending year
  rank:           <int>     // T-Rank national rank
  conf:           <string>  // Conference abbreviation
  record:         <string>  // Win-loss record (e.g. "28-7")
  adjoe:          <float>
  adjde:          <float>
  barthag:        <float>
  efg_pct:        <float>
  efg_d_pct:      <float>
  tor:            <float>
  tord:           <float>
  orb:            <float>
  drb:            <float>
  ftr:            <float>
  ftrd:           <float>
  two_pt_pct:     <float>
  two_pt_d_pct:   <float>
  three_pt_pct:   <float>
  three_pt_d_pct: <float>
  adj_tempo:      <float>
  wab:            <float>
  ingested_at:    <ISODate>
}
```

### Data Summary

| Collection | Approximate Documents | Seasons Covered | Key Join Key |
|---|---|---|---|
| `games` | ~4,000–6,000 | 2015–2024 | `(season, winner.team, loser.team, date)` |
| `team_ratings` | ~3,500 (350 teams × 10 seasons) | 2015–2024 | `(team, season)` |

> Update the document counts above after running `04_merge_and_ingest.py`.

### Data Dictionary — `games` Collection (Selected Features)

| Field | Type | Description | Example | Uncertainty |
|---|---|---|---|---|
| `season` | int | Ending calendar year of the NCAA season | `2024` | None — deterministic from scrape year |
| `date` | string | Date of the game | `"2023-11-10"` | Low — parsed directly from Sports-Reference; rare missing values for older games |
| `winner.team` | string | Name of the winning team | `"Duke"` | Low — direct from source; occasional name variant mismatches with Barttorvik |
| `winner.pts` | float | Points scored by the winner | `78.0` | Very low — official game scores |
| `winner.dist_miles` | float | Estimated travel distance for the winner (miles) | `412.3` | **High** — midpoint venue heuristic introduces error of 50–300 miles for some games |
| `winner.ratings.adjoe` | float | Winner's adjusted offensive efficiency at time of season | `118.4` | Medium — season-level aggregate; does not reflect team form at game date |
| `winner.ratings.adjde` | float | Winner's adjusted defensive efficiency | `94.1` | Medium — same caveat as adjoe |
| `winner.ratings.barthag` | float | Winner's power rating (prob. of beating avg D1 team) | `0.937` | Medium — model-derived; uncertainty compounds from all component metrics |
| `winner.ratings.adj_tempo` | float | Adjusted possessions per 40 minutes | `71.2` | Low-medium — stable across a season; small sample variance early in year |
| `winner.ratings.wab` | float | Wins above bubble | `8.4` | Medium — schedule-dependent; high variance for teams with few games |
| `loser.dist_miles` | float | Estimated travel distance for the loser (miles) | `638.7` | **High** — same midpoint heuristic caveat as winner |
| `derived.dist_diff_miles` | float | winner_dist_miles − loser_dist_miles (positive = winner traveled farther) | `-226.4` | **High** — inherits error from both distance estimates |
| `derived.barthag_diff` | float | winner_barthag − loser_barthag | `0.312` | Medium — propagated from both teams' barthag uncertainty |
| `derived.adjoe_diff` | float | Offensive efficiency gap between winner and loser | `12.1` | Medium — season aggregate; see adjoe note |
| `derived.point_diff` | float | Final score margin (winner_pts − loser_pts) | `11.0` | Very low — official game scores |
| `venue.lat` / `venue.lon` | float | Estimated venue coordinates (midpoint heuristic) | `38.5, -90.2` | **High** — see midpoint heuristic discussion in Data Creation |
| `ingested_at` | ISODate | UTC timestamp of when document was written to MongoDB | `2025-03-15T14:22:00Z` | None — system-generated |

# Home Is Where the West Is: Do NCAA Tournament Teams Perform Better on Their Own Turf?

## New Research Finds Geography Plays a Measurable Role in NCAA West Region Outcomes

A new data science analysis of a decade of NCAA Tournament West Region games finds that teams located further west than their opponent win more often — and that this geographic signal is statistically detectable even after accounting for team quality and seeding.

---

## The Problem

Every March, 64 teams compete in the NCAA Tournament, and every year millions of fans fill out brackets that are almost immediately wrong. Most prediction models focus on team quality: efficiency ratings, strength of schedule, offensive and defensive metrics. But one factor is almost never discussed — where each team calls home.

The NCAA Tournament assigns games to regional venues across the country. The West Region is consistently held in cities like Las Vegas, Los Angeles, San Francisco, San Diego, and Salt Lake City. Teams from the Pacific Coast, the Mountain West, and the Southwest travel a few hundred miles. Teams from the Southeast, the Big Ten, or the Atlantic Coast travel two thousand miles, cross two or three time zones, and compete in arenas filled largely with fans rooting against them.

Does any of that matter? Or does talent always win out?

---

## The Solution

To answer this question, a secondary dataset was constructed from ten years of NCAA Tournament West Region game results (2015–2025), combined with Barttorvik T-Rank efficiency ratings for every participating team. For each game, the longitude of each team's home campus was geocoded and compared to the longitude of that year's West Region venue city — giving a precise measure of how much further west or east each team was relative to its opponent.

The result is a document database of 133 games stored in MongoDB Atlas, each enriched with team quality metrics, travel distances, seeding, and the key hypothesis variable: longitude difference.

A machine learning pipeline then tested two models head-to-head. Model A predicted game outcomes using team quality alone — adjusted offensive efficiency, adjusted defensive efficiency, power rating, and seed. Model B added geography — longitude difference and travel distance. If geography matters, Model B should outperform Model A.

---

## The Findings

### Geography is real, but quality still dominates

The data supports the hypothesis — partially. In the 76 games with complete ratings data, the winner was located further west than the loser in **59.2% of games** — notably above the 50% rate expected if geography had no effect at all.

The correlation between longitude difference and point margin is **r = −0.239 (p = 0.038)**, statistically significant at the standard α = 0.05 threshold. The negative sign is exactly what the hypothesis predicts: as a winner becomes further west relative to their opponent, their margin of victory increases.

Adding geography to the prediction model improved performance across all three classifiers tested. Logistic Regression ROC-AUC improved from **0.893 to 0.917**. Random Forest improved from **0.736 to 0.810**. Longitude difference ranked 4th out of 6 features in the Gradient Boosting model — ahead of travel distance, confirming that raw longitudinal position matters more than miles traveled to the venue.

### But it doesn't overcome a bad matchup

The t-test comparing point margins for western vs. eastern winners did not reach significance (p = 0.111), and team quality features — barthag difference and seed difference — remain the dominant predictors in every model. A 1-seed from Duke is still going to beat a 16-seed from a California school. Geography tilts the playing field; it does not flip it.

---

## A Chart of the Data

The scatter plot below shows the relationship between longitude difference (x-axis) and point margin (y-axis) for all 76 modeled West Region games. Blue points are games where the winner was further west; red points are games where the winner was further east. The gold trend line shows the statistically significant negative slope — further west correlates with winning by more.

> 📊 See `figures/01_eda_longitude.png` in the pipeline output for the full visualization.

The distribution of longitude differences also tells a story: the mean `lon_diff` is **−3.9 degrees**, meaning the average winner in this dataset is located slightly west of the average loser — a small but consistent directional effect across a decade of games.

---

## Why It Matters

For fans and analysts building brackets, this finding suggests that geographic matchups in the West Region deserve more weight than they currently receive. A mid-major program from Gonzaga, Nevada, or San Diego State playing close to home may be systematically undervalued by models that consider only efficiency ratings.

For the NCAA, it raises a fairness question: if regional venue assignments create systematic geographic advantages, should seeding committees account for this when constructing brackets?

For researchers, this study provides a replicable framework — a clean document database, an open-source pipeline, and a transparent methodology — for extending this analysis to the other three regions and testing whether the effect is West-specific or a general feature of tournament geography.

---

*Dataset and full analysis pipeline available at: [github.com/Kieran-11/DS4320-Project-2](https://github.com/Kieran-11/DS4320-Project-2)*
*Kieran Perdue — DS 4320, Spring 2026*

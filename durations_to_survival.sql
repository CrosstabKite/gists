
-- Author: Brian Kent, The Crosstab Kite

-- Compute Kaplan-Meier survival curves and Nelson-Aalen cumulative hazard
-- curves in SQL. Accompanies the article at
-- https://www.crosstab.io/articles/sql-survival-curves.

-- Explore the durations table again as a refresher.
select * from durations limit 5;

select count(1) from durations;

select count(1) from durations where endpoint_type is not null;


-- The main query
CREATE TABLE survival AS

WITH num_subjects AS (
    SELECT COUNT(1) AS num_subjects FROM durations
),

-- Convert interval-type durations to numeric, by roudning up to nearest whole
-- duration day.
duration_rounded AS (
    SELECT 
        visitorid,
        endpoint_type,
        duration,
        ceil(extract(epoch FROM duration)/(24 * 60 * 60)) AS duration_days
    FROM durations
),

-- Count number of observations and experienced outcome events on each duration.
daily_tally AS (
    SELECT
        duration_days,
        COUNT(1) AS num_obs,
        SUM(
            CASE
                WHEN endpoint_type IS NOT NULL THEN 1
                ELSE 0
            END
        ) AS events
    FROM duration_rounded
    GROUP BY 1
),


-- Count number of subjects still at risk at each duration. Subtract the running
-- total of observations prior to each row, which includes both observed and
-- censored durations.
cumulative_tally AS (
    SELECT 
        duration_days,
        num_obs,
        events,
        num_subjects - COALESCE(SUM(num_obs) OVER (
            ORDER BY duration_days ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0
        ) AS at_risk
    FROM daily_tally, num_subjects
)

-- 1. Censored explanation, can't just subtract events FROM obs because the
--    WHERE clause preceeds the column opereations ("Aggregations"), logically,
--    and we need to account for the censored obs in the rows that are dropped.
--    1A. Coalesce to fill in 0 for the last row, which is null because of the
--    lead function.
-- 2. Kaplan-Meier explanation: no product for aggregations, so hack it WITH
--    exp(sum(log(.)))
SELECT
    duration_days,
    at_risk,
    num_obs,
    events,
    at_risk - events - COALESCE(lead(at_risk, 1) OVER (ORDER BY duration_days ASC), 0) AS censored,

    EXP(SUM(LN(1 - events / at_risk)) OVER (
        ORDER BY duration_days ASC ROWS BETWEEN UNBOUNDED PRECEDING AND current ROW
    )) AS survival_proba,

    100 * (1 - EXP(SUM(LN(1 - events / at_risk)) OVER (
        ORDER BY duration_days ASC ROWS BETWEEN UNBOUNDED PRECEDING AND current ROW
    ))) AS conversion_pct,

    SUM(events / at_risk) OVER (
        ORDER BY duration_days ASC ROWS BETWEEN UNBOUNDED PRECEDING AND current ROW
    ) AS cumulative_hazard

FROM cumulative_tally
WHERE events > 0;


-- Print specific output for the article.
SELECT
    duration_days,
    at_risk,
    num_obs,
    events,
    censored
FROM survival
ORDER BY duration_days ASC
LIMIT 5;

SELECT * FROM (
    SELECT
        duration_days,
        TRUNC(survival_proba, 4) AS survival_proba,
        TRUNC(conversion_pct, 4) AS conversion_pct,
        TRUNC(cumulative_hazard, 4) AS cumulative_hazard
    FROM survival
    ORDER BY duration_days DESC
    LIMIT 5
) AS tail
ORDER BY duration_days ASC;

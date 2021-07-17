
-- GENERAL NOTES
-- CTEs
-- Useful for analytics dashboards that don't run on Python
-- Useful for big jobs that are easier to run as queries against a data warehouse

-- -- Explore the durations table again as a refresher.
-- select * from durations limit 5;

-- select count(1) from durations;

-- select count(1) from durations where endpoint_type is not null;


-- Convert the durations, which have type `interval` to integer days, by
-- rounding up.
WITH num_subjects AS (
    SELECT COUNT(1) AS num_subjects FROM durations
),

-- Explain the interepretation of rounding up to nearest whole duration day.
duration_rounded AS (
    SELECT 
        visitorid,
        endpoint_type,
        duration,
        ceil(extract(epoch FROM duration)/(24 * 60 * 60)) AS duration_days
    FROM durations
),

-- Explain tally of events AS number of non-null entries.
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

-- FROM two tables does cross-product. Since num_subjects is a single value, this effectively broadcasts it to every row.
-- at_risk explanation: subtract running total of observations prior to each row, which includes both observed events and censored durations.
cumulative_tally AS (
    SELECT 
        duration_days,
        num_obs,
        events,
        num_subjects - COALESCE(SUM(num_obs) OVER (ORDER BY duration_days ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS at_risk
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
    EXP(SUM(LN(1 - events / at_risk)) OVER (ORDER BY duration_days ASC ROWS BETWEEN UNBOUNDED PRECEDING AND current ROW)) AS survival_proba,
    100 * (1 - EXP(SUM(LN(1 - events / at_risk)) OVER (ORDER BY duration_days ASC ROWS BETWEEN UNBOUNDED PRECEDING AND current ROW))) AS conversion_pct,
    SUM(events / at_risk) OVER (ORDER BY duration_days ASC ROWS BETWEEN UNBOUNDED PRECEDING AND current ROW) AS cumulative_hazard
FROM cumulative_tally
WHERE events > 0;

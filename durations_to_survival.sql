
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
with num_subjects as (
    select count(1) as num_subjects from durations
),

-- Explain the interepretation of rounding up to nearest whole duration day.
duration_rounded as (
    select 
        visitorid,
        endpoint_type,
        duration,
        ceil(extract(epoch from duration)/(24 * 3600)) as duration_days
    from durations
),

-- Explain tally of events as number of non-null entries.
daily_tally as (
    select
        duration_days,
        count(1) as num_obs,
        sum(
            case
                when endpoint_type is not null then 1
                else 0
            end
        ) as events
    from duration_rounded
    group by 1
),

-- FROM two tables does cross-product. Since num_subjects is a single value, this effectively broadcasts it to every row.
-- at_risk explanation: subtract running total of observations prior to each row, which includes both observed events and censored durations.
cumulative_tally as (
    select 
        duration_days,
        num_obs,
        events,
        num_subjects - coalesce(sum(num_obs) over (order by duration_days asc rows between unbounded preceding and 1 preceding), 0) as at_risk
    from daily_tally, num_subjects
)

-- 1. Censored explanation, can't just subtract events from obs because the
--    WHERE clause preceeds the column opereations ("Aggregations"), logically,
--    and we need to account for the censored obs in the rows that are dropped.
--    1A. Coalesce to fill in 0 for the last row, which is null because of the
--    lead function.
-- 2. Kaplan-Meier explanation: no product for aggregations, so hack it with
--    exp(sum(log(.)))
select
    duration_days,
    at_risk,
    num_obs,
    events,
    at_risk - events - coalesce(lead(at_risk, 1) over (order by duration_days asc), 0) as censored,
    exp(sum(ln(1 - events / at_risk)) over (order by duration_days asc rows between unbounded preceding and current row)) as survival_proba,
    100 * (1 - exp(sum(ln(1 - events / at_risk)) over (order by duration_days asc rows between unbounded preceding and current row))) as conversion_pct,
    sum(events / at_risk) over (order by duration_days asc rows between unbounded preceding and current row) as cumulative_hazard
from cumulative_tally
where events > 0;

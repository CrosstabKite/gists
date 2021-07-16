
-- Explore the durations table again as a refresher.
select * from durations limit 5;

select count(1) from durations;

select count(1) from durations where endpoint_type is not null;


-- Convert the durations, which have type `interval` to integer days, by
-- rounding up.
with duration_rounded as (
    select 
        visitorid,
        endpoint_type,
        duration,
        ceil(extract(epoch from duration)/(24 * 3600)) as duration_days
    from durations
),

-- Verify that 118 users have duration_days == 0.
-- select * from duration_days where duration_days < 1;

-- select
--     visitorid,
--     endpoint_type,
--     case
--         when endpoint_type is null then 0
--         else 1
--     end
-- from durations;

daily_tally as (
    select
        duration_days,
        count(1) as num_obs,
        sum(
            case
                when endpoint_type is null then 0
                else 1
            end
        ) as events
    from duration_rounded
    group by 1
),

cumulative_tally as (
    select 
        duration_days,
        num_obs,
        events,
        1407580 - coalesce(sum(num_obs) over (order by duration_days asc rows between unbounded preceding and 1 preceding), 0) as at_risk
    from daily_tally
)

select
    *,
    at_risk - events - coalesce(lead(at_risk, 1) over (order by duration_days asc), 0) as censored,
    sum(events / at_risk) over (order by duration_days asc rows between unbounded preceding and current row) as cumulative_hazard
from cumulative_tally
where events > 0;

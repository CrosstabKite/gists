
-- Drop existing table (only for dev)
DROP TABLE event_log;


-- Create the table
CREATE TABLE event_log (
    unix_timestamp NUMERIC,
    visitorid NUMERIC, 
    event VARCHAR, 
    itemid VARCHAR, 
    transactionid VARCHAR
);


-- Copy the data in from local CSV
COPY event_log 
FROM '/home/brian/crosstab/projects/gists/data/retailrocket/events.csv' 
DELIMITER ',' 
CSV HEADER;


-- Confirm the data loaded
SELECT count(1) FROM event_log;


-- Create a timestamp column. This wouldn't necessarily be a good idea in prod,
-- but it keeps things clean for demos.
-- TODO: need to figure out why this is off by 5 hours.
ALTER TABLE event_log ADD column timestamp TIMESTAMP WITH TIME ZONE;
UPDATE event_log SET timestamp = to_timestamp(unix_timestamp / 1000);


-- Count number of each type of event
SELECT
    event,
    count(1) as num_observations
FROM event_log
GROUP BY 1
ORDER BY 2 DESC;


-- View all entries for a visitor who has an endpoint.
SELECT
    *
FROM event_log 
WHERE visitorid='1050575' 
ORDER BY timestamp ASC;


-- --------------
-- Duration table construction starts here.
-- --------------

-- Find the entry time for each unit.
WITH entry_times AS (
    SELECT
        visitorid,
        min(timestamp) AS timestamp
    FROM event_log
    GROUP BY 1
),

-- Get the earliest endpoint event for units that have an endpoint.
endpoint_events AS (
    SELECT *
    FROM event_log
    WHERE event IN ('transaction')
),

first_endpoint_events AS (
    SELECT 
        *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY visitorid ORDER BY timestamp ASC) AS row_num
        FROM endpoint_events
    ) AS _
    WHERE row_num = 1
),

-- Define the censoring time to be the latest timestamp in the whole event log.
censoring AS (
    SELECT max(timestamp) AS timestamp FROM event_log
)

-- Put all the pieces together as a *duration table*.
SELECT 
    entry_times.visitorid,
    entry_times.timestamp as entry_time,
    endpt.event AS endpoint,
    endpt.timestamp AS endpoint_time,
    COALESCE(endpt.timestamp, censoring.timestamp) as final_obs_time,
    COALESCE(endpt.timestamp, censoring.timestamp) - entry_times.timestamp as duration
FROM censoring, entry_times
LEFT JOIN first_endpoint_events AS endpt
    USING(visitorid)
LIMIT 5;

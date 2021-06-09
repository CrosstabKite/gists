
-- Drop existing table (only for dev)
DROP TABLE event_log;


-- Create the table
CREATE TABLE event_log (
    unix_timestamp NUMERIC,
    visitorid NUMERIC, 
    event_type VARCHAR, 
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


-- Create a timestamp column and drop the original unix epoch column. This
-- wouldn't necessarily be a good idea in prod, but it keeps things clean for
-- the article.
ALTER TABLE event_log ADD column event_at TIMESTAMP WITH TIME ZONE;
UPDATE event_log SET event_at = to_timestamp(unix_timestamp / 1000);
ALTER TABLE event_log DROP COLUMN unix_timestamp;


-- Show the head of the data.
SELECT * FROM event_log LIMIT 5;


-- Count number of each type of event (implicit limit 5)
SELECT
    event_type,
    count(1) as num_observations
FROM event_log
GROUP BY 1
ORDER BY 2 DESC


-- View all entries for a visitor who has an endpoint (implicit limit 5)
SELECT
    *
FROM event_log 
WHERE visitorid='1050575' 
ORDER BY event_at ASC


-- --------------
-- Duration table construction starts here.
-- --------------

-- Find the entry time for each unit.
WITH entry_times AS (
    SELECT
        visitorid,
        min(event_at) AS event_at
    FROM event_log
    GROUP BY 1
),

-- Get the earliest endpoint event for units that have an endpoint.
endpoint_events AS (
    SELECT *
    FROM event_log
    WHERE event_type IN ('transaction')
),

first_endpoint_events AS (
    SELECT 
        *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER(PARTITION BY visitorid ORDER BY event_at ASC) AS row_num
        FROM endpoint_events
    ) AS _
    WHERE row_num = 1
),

-- Define the censoring time to be the latest timestamp in the whole event log.
censoring AS (
    SELECT max(event_at) AS event_at FROM event_log
)

-- Put all the pieces together as a *duration table* (imlicit limit 5).
SELECT 
    entry_times.visitorid,
    entry_times.event_at as entry_at,
    endpt.event_type AS endpoint_type,
    endpt.event_at AS endpoint_at,
    COALESCE(endpt.event_at, censoring.event_at) as final_obs_at,
    COALESCE(endpt.event_at, censoring.event_at) - entry_times.event_at as duration
FROM censoring, entry_times
LEFT JOIN first_endpoint_events AS endpt
    USING(visitorid)


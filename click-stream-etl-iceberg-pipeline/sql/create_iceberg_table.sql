-- ===================================================
-- Create Iceberg Table for Clickstream Events
-- Based on Parquet files from Glue Streaming Job
-- Database: iceberg-click-stream
-- Table: clickstream_events
-- ===================================================

DROP TABLE IF EXISTS iceberg_click_stream.clickstream_events;

CREATE TABLE iceberg_click_stream.clickstream_events (
    run_id STRING,
    event_id STRING,
    user_id INT,
    session_id STRING,
    country STRING,
    device STRING,
    event_type STRING,
    page STRING,
    click_time TIMESTAMP,
    leave_time TIMESTAMP,
    session_duration_sec INT,
    ingestion_timestamp TIMESTAMP
) USING ICEBERG PARTITIONED BY (country) LOCATION 's3://iceberg-data-lake-prep-demo/data/';

-- ===================================================
-- Alternative: Create table with Iceberg Catalog
-- Use this if running through AWS Glue Catalog
-- ===================================================

-- CREATE TABLE IF NOT EXISTS iceberg_click_stream.clickstream_events (
--     run_id STRING,
--     event_id STRING,
--     user_id INT,
--     session_id STRING,
--     country STRING,
--     device STRING,
--     event_type STRING,
--     page STRING,
--     click_time TIMESTAMP,
--     leave_time TIMESTAMP,
--     session_duration_sec INT,
--     ingestion_timestamp TIMESTAMP
-- )
-- USING iceberg
-- PARTITIONED BY (country);

-- ===================================================
-- Query Iceberg Table
-- ===================================================

-- SELECT * FROM iceberg_click_stream.clickstream_events LIMIT 10;

-- ===================================================
-- Time Travel Query
-- ===================================================

-- SELECT * FROM iceberg_click_stream.clickstream_events
-- FOR SYSTEM_TIME AS OF '2026-02-07T10:00:00Z'
-- WHERE country = 'IN';

-- ===================================================
-- View Table Snapshots
-- ===================================================

-- SELECT * FROM iceberg_click_stream.clickstream_events.snapshots;

-- ===================================================
-- View Table Schema Evolution
-- ===================================================

-- SELECT * FROM iceberg_click_stream.clickstream_events.schemas;
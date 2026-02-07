DROP TABLE IF EXISTS clickstream_db.clickstream_parquet;

CREATE EXTERNAL TABLE clickstream_db.clickstream_parquet (
    run_id STRING,
    event_id STRING,
    user_id INT,
    session_id STRING,
    device STRING,
    event_type STRING,
    page STRING,
    click_time TIMESTAMP,
    leave_time TIMESTAMP,
    session_duration_sec INT
)
PARTITIONED BY (country STRING)
STORED AS PARQUET
LOCATION 's3://interview-prep-bucket/click-stream-etl-pipeline/clickstream/';
--MSCK REPAIR TABLE clickstream_db.clickstream_parquet;
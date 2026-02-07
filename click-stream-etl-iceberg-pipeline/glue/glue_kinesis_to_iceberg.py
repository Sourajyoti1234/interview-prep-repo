import sys
import os
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import *

# --------------------------------------------------
# Glue Job Args
# --------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'KINESIS_STREAM_NAME', 'DATABASE_NAME', 'S3_OUTPUT_PATH']
)

# --------------------------------------------------
# Init Spark + Glue Context
# --------------------------------------------------
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --------------------------------------------------
# Schema
# --------------------------------------------------
schema = StructType([
    StructField("run_id", StringType()),
    StructField("event_id", StringType()),
    StructField("user_id", IntegerType()),
    StructField("session_id", StringType()),
    StructField("country", StringType()),
    StructField("device", StringType()),
    StructField("event_type", StringType()),
    StructField("page", StringType()),
    StructField("click_time", TimestampType()),
    StructField("leave_time", TimestampType()),
    StructField("session_duration_sec", IntegerType())
])

# --------------------------------------------------
# Read from Kinesis (Streaming)
# --------------------------------------------------
kinesis_df = (
    spark.readStream
    .format("kinesis")
    .option("streamName", args['KINESIS_STREAM_NAME'])
    .option("region", "us-east-1")
    .option("initialPosition", "LATEST")
    .load()
)

# --------------------------------------------------
# Parse JSON
# --------------------------------------------------
parsed_df = (
    kinesis_df
    .selectExpr("CAST(data AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("ingestion_timestamp", current_timestamp())
)

# --------------------------------------------------
# Write to Parquet Files via foreachBatch
# --------------------------------------------------


def write_to_parquet(batch_df, batch_id):
    """
    Write batch data to S3 in Parquet format
    Partitioned by country for easy access
    """
    try:
        record_count = batch_df.count()

        print(f"Batch ID: {batch_id}")
        print(f"Total records: {record_count}")

        if record_count > 0:
            print("Sample records:")
            batch_df.show(5, truncate=False)

            s3_output_path = args['S3_OUTPUT_PATH']

            # Write to S3 in Parquet format
            (
                batch_df
                .write
                .mode("append")
                .format("parquet")
                .partitionBy("country")
                .save(s3_output_path)
            )

            print(
                f"Successfully written {record_count} records to {s3_output_path}")

    except Exception as e:
        print(f"Error writing batch {batch_id}: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


# --------------------------------------------------
# Start Streaming with Parquet Writer
# --------------------------------------------------
query = (
    parsed_df
    .writeStream
    .foreachBatch(write_to_parquet)
    .option("checkpointLocation", f"{args['S3_OUTPUT_PATH']}/checkpoints/clickstream/")
    .start()
)

query.awaitTermination()
job.commit()

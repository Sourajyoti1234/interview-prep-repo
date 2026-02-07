import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

# --------------------------------------------------
# Glue Job Args
# --------------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

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
    .option("streamName", "kinesis-click-real-data")
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
)

# --------------------------------------------------
# foreachBatch logic
# --------------------------------------------------


def write_to_s3(batch_df, batch_id):
    record_count = batch_df.count()

    print(f"Batch ID: {batch_id}")
    print(f"Total records inserted: {record_count}")

    if record_count > 0:
        print("Sample records:")
        batch_df.show(5, truncate=False)

        (
            batch_df
            .write
            .mode("append")
            .partitionBy("country")
            .parquet(
                "s3://iceberg-data-lake-nm-demo/glue-stream-kinesis/clickstream"
            )
        )


# --------------------------------------------------
# Start Streaming
# --------------------------------------------------
query = (
    parsed_df
    .writeStream
    .foreachBatch(write_to_s3)
    .option(
        "checkpointLocation",
        "s3://iceberg-data-lake-nm-demo/glue-stream-kinesis/_chk/"
    )
    .start()
)

query.awaitTermination()
job.commit()

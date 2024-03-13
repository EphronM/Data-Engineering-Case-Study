from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import when

def process_ad_impressions(ad_impressions_df, batch_id):
    # Basic data processing for ad impressions
    ad_impressions_processed_df = ad_impressions_df \
        .withColumn("processed_data", when(col("ad_creative_id").isNull(), "invalid").otherwise("valid"))

    # Show processed data and write to storage
    print(f"Processed Batch-{batch_id} Ad Impressions:")
    ad_impressions_processed_df.show(5)

    # Store processed data locally (example: writing to Parquet files)
    ad_impressions_processed_df.write.mode('append').parquet("processed_data/ad_impressions")

def process_clicks_conversions(clicks_conversions_df, batch_id):
    # Basic data processing for clicks/conversions
    clicks_conversions_processed_df = clicks_conversions_df \
        .withColumn("processed_data", when(col("ad_campaign_id").isNull(), "invalid").otherwise("valid"))

    # Show processed data and write to storage
    print(f"Processed-{batch_id}  Clicks/Conversions:")
    clicks_conversions_processed_df.show(5)

    # Store processed data locally (example: writing to Parquet files)
    clicks_conversions_processed_df.write.mode('append').parquet("processed_data/clicks_conversions")

def process_bid_requests(bid_requests_df, batch_id):
    # Basic data processing for bid requests
    bid_requests_processed_df = bid_requests_df \
        .withColumn("processed_data", when(col("advertiser_id").isNull(), "invalid").otherwise("valid"))

    # Show processed data and write to storage
    print(f"Processed-{batch_id}  Bid Requests:")
    bid_requests_processed_df.show(5)

    # Store processed data locally (example: writing to Parquet files)
    bid_requests_processed_df.write.mode('append').parquet("processed_data/bid_requests")

def setup_error_handling():
    # Example: Set up error handling and monitoring (placeholder)
    print("Error handling and monitoring setup")

if __name__ == "__main__":
    # Spark session
    spark = SparkSession.builder \
        .appName("RealTimeDataProcessing") \
        .getOrCreate()

    # Kafka configuration
    kafka_bootstrap_servers = 'localhost:9092'  # Update with your Kafka broker addresses
    topic_ad_impressions = 'ad_impressions'
    topic_clicks_conversions = 'clicks_conversions'
    topic_bid_requests = 'bid_requests'

    # Subscribe to Kafka topics
    ad_impressions_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic_ad_impressions) \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    clicks_conversions_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic_clicks_conversions) \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    bid_requests_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic_bid_requests) \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    # Convert JSON strings to DataFrame
    ad_impressions_schema = StructType().add("ad_creative_id", StringType()) \
        .add("user_id", StringType()) \
        .add("timestamp", StringType()) \
        .add("website", StringType())
    ad_impressions_df = ad_impressions_df \
        .select(from_json(col("value"), ad_impressions_schema).alias("data")) \
        .select("data.*")

    clicks_conversions_schema = StructType().add("timestamp", StringType()) \
        .add("user_id", StringType()) \
        .add("ad_campaign_id", StringType()) \
        .add("conversion_time", StringType()) \
        .add("conversion_type", StringType())
    clicks_conversions_df = clicks_conversions_df \
        .select(from_json(col("value"), clicks_conversions_schema).alias("data")) \
        .select("data.*")

    bid_requests_schema = StructType().add("user_id", StringType()) \
        .add("auction_id", StringType()) \
        .add("timestamp", StringType()) \
        .add("advertiser_id", StringType()) \
        .add("ad_size", StringType()) \
        .add("device_type", StringType()) \
        .add("bid_amount", StringType())
    bid_requests_df = bid_requests_df \
        .select(from_json(col("value"), bid_requests_schema).alias("data")) \
        .select("data.*")

    # Process the incoming data streams
    ad_impressions_df.writeStream \
        .foreachBatch(process_ad_impressions) \
        .start()

    clicks_conversions_df.writeStream \
        .foreachBatch(process_clicks_conversions) \
        .start()

    bid_requests_df.writeStream \
        .foreachBatch(process_bid_requests) \
        .start()

    # Set up error handling and monitoring
    setup_error_handling()

    # Await termination
    spark.streams.awaitAnyTermination()

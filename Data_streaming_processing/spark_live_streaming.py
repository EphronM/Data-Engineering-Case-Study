from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import when
from pyspark.sql.functions import to_timestamp, month, lit
from prometheus_client import start_http_server, Counter

# Define Prometheus metrics
processed_ad_impressions_counter = Counter('processed_ad_impressions', 'Number of processed ad impressions')
processed_clicks_conversions_counter = Counter('processed_clicks_conversions', 'Number of processed clicks/conversions')
processed_bid_requests_counter = Counter('processed_bid_requests', 'Number of processed bid requests')




def process_ad_impressions(ad_impressions_df, batch_id):
    # Extract month from timestamp
    ad_impressions_df = ad_impressions_df.withColumn("month", month(to_timestamp("timestamp")))
    
    # Add a new field
    ad_impressions_df = ad_impressions_df.withColumn("platform", lit("web"))
    
    # Print the modified DataFrame
    print(f"Processed Ad Impressions batch #{batch_id}:")
    ad_impressions_df.show(5)

    # Increment Prometheus counter
    processed_ad_impressions_counter.inc(ad_impressions_df.count())

    # Store processed data locally
    ad_impressions_df.write.mode('append').parquet("processed_data/ad_impressions")


def process_clicks_conversions(clicks_conversions_df, batch_id):
    # Extract month from timestamp
    clicks_conversions_df = clicks_conversions_df.withColumn("month", month(to_timestamp("timestamp")))
    
    # Add a new field
    clicks_conversions_df = clicks_conversions_df.withColumn("event_type", lit("click"))
    
    # Print the modified DataFrame
    print(f"Processed Clicks/Conversions batch #{batch_id}:")
    clicks_conversions_df.show(5)

    # Increment Prometheus counter
    processed_clicks_conversions_counter.inc(clicks_conversions_df.count())

    # Store processed data locally
    clicks_conversions_df.write.mode('append').parquet("processed_data/clicks_conversions")

def process_bid_requests(bid_requests_df, batch_id):
    # Extract month from timestamp
    bid_requests_df = bid_requests_df.withColumn("month", month(to_timestamp("timestamp")))
    
    # Add a new field
    bid_requests_df = bid_requests_df.withColumn("region", lit("US"))
    
    # Print the modified DataFrame
    print(f"Processed Bid Requests batch #{batch_id}:")
    bid_requests_df.show(5)

    # Increment Prometheus counter
    processed_bid_requests_counter.inc(bid_requests_df.count())

    # Store processed data locally
    bid_requests_df.write.mode('append').parquet("processed_data/bid_requests")


def setup_error_handling():
    # Example: Set up error handling and monitoring (placeholder)
    print("Error handling and monitoring setup")

if __name__ == "__main__":
    # Start Prometheus server
    start_http_server(8000)

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

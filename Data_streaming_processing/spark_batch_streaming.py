import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, date_format, to_timestamp, hour, when
from pyspark.sql.types import StructType, StringType
from prometheus_client import start_http_server, Counter

# Define Prometheus metrics
processed_ad_impressions_counter = Counter('processed_ad_impressions', 'Number of processed ad impressions')
processed_clicks_conversions_counter = Counter('processed_clicks_conversions', 'Number of processed clicks/conversions')
processed_bid_requests_counter = Counter('processed_bid_requests', 'Number of processed bid requests')

def process_ad_impressions(ad_impressions_df, batch_id):
    batch_name = get_batch_name("Data_storage/Batch_data/ad_impressions")
    batch_dir = f"Data_storage/Batch_data/ad_impressions/{batch_name}"
    create_directory_if_not_exists(batch_dir)
    
    # Extract month from timestamp
    ad_impressions_df = ad_impressions_df.withColumn("month", date_format(to_timestamp("timestamp"), "MMMM"))

    # Add a new field to represent the day of the week
    ad_impressions_df = ad_impressions_df.withColumn("day_of_week", date_format(to_timestamp("timestamp"), "EEEE"))
    
    # Add a new field
    ad_impressions_df = ad_impressions_df.withColumn("platform", lit("web"))
    
    # Print the modified DataFrame
    print(f"Processed Ad Impressions batch #{batch_id}:")
    ad_impressions_df.show(5)

    # Increment Prometheus counter
    processed_ad_impressions_counter.inc(ad_impressions_df.count())

    # Store processed data locally with batch_name
    ad_impressions_df.write.mode('append').parquet(batch_dir)

def process_clicks_conversions(clicks_conversions_df, batch_id):
    batch_name = get_batch_name("Data_storage/Batch_data/clicks_conversions")
    batch_dir = f"Data_storage/Batch_data/clicks_conversions/{batch_name}"
    create_directory_if_not_exists(batch_dir)
    
    # Extract month from timestamp
    clicks_conversions_df = clicks_conversions_df.withColumn("month", date_format(to_timestamp("timestamp"), "MMMM"))
    
    # Add a new field
    clicks_conversions_df = clicks_conversions_df.withColumn("event_type", lit("click"))

    # Categorize periods of the day directly
    clicks_conversions_df = clicks_conversions_df.withColumn("period_of_day", 
        when((hour(to_timestamp("conversion_time")) >= 0) & (hour(to_timestamp("conversion_time")) < 6), "night")
        .when((hour(to_timestamp("conversion_time")) >= 6) & (hour(to_timestamp("conversion_time")) < 12), "morning")
        .when((hour(to_timestamp("conversion_time")) >= 12) & (hour(to_timestamp("conversion_time")) < 18), "afternoon")
        .otherwise("evening")
    )
    
    # Print the modified DataFrame
    print(f"Processed Clicks/Conversions batch #{batch_id}:")
    clicks_conversions_df.show(5)

    # Increment Prometheus counter
    processed_clicks_conversions_counter.inc(clicks_conversions_df.count())

    # Store processed data locally with batch_name
    clicks_conversions_df.write.mode('append').parquet(batch_dir)

def process_bid_requests(bid_requests_df, batch_id):
    batch_name = get_batch_name("Data_storage/Batch_data/bid_requests")
    batch_dir = f"Data_storage/Batch_data/bid_requests/{batch_name}"
    create_directory_if_not_exists(batch_dir)
    
    # Extract month from timestamp
    bid_requests_df = bid_requests_df.withColumn("month", date_format(to_timestamp("timestamp"), "MMMM"))

    # Categorize bid amount into sections
    bid_requests_df = bid_requests_df.withColumn("bid_category", 
        when(col("bid_amount") <= 2, "small")
        .when((col("bid_amount") > 2) & (col("bid_amount") <= 5), "medium")
        .otherwise("large")
    )
    
    # Add a new field
    bid_requests_df = bid_requests_df.withColumn("region", lit("US"))
    
    # Print the modified DataFrame
    print(f"Processed Bid Requests batch #{batch_id}:")
    bid_requests_df.show(5)

    # Increment Prometheus counter
    processed_bid_requests_counter.inc(bid_requests_df.count())

    # Store processed data locally with batch_name
    bid_requests_df.write.mode('append').parquet(batch_dir)

def check_directory(directory):
    return os.path.exists(directory)

def get_batch_name(directory):
    batch_names = []
    if check_directory(directory):
        for file_name in os.listdir(directory):
            match = re.match(r'^Data_batch_(\d+)', file_name)
            if match:
                batch_names.append(int(match.group(1)))
    if batch_names:
        latest_batch_number = max(batch_names)
        return f"Data_batch_{latest_batch_number + 1}"
    else:
        return "Data_batch_0"

def create_directory_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

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

    # Await termination
    spark.streams.awaitAnyTermination()
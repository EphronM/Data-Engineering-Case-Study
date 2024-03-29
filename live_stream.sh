#!/bin/bash

# Start Kafka server
echo " >> Starting Kafka server..."
cd kafka_2.13-3.7.0/
bin/kafka-server-start.sh config/server.properties &

# Wait for Kafka server to start
sleep 10

# moving to current working directory
cd ..



# Start Spark Streaming application
echo " >> Starting Spark Streaming application..."
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 Data_streaming_processing/spark_live_streaming.py &

# Wait for Spark Streaming application to start
sleep 20

# Run Streamlit application
echo " >> Running Streamlit application..."
streamlit run live_app.py


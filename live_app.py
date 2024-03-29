import sys
import time
import json
import random
import datetime
import streamlit as st
from kafka import KafkaProducer
from Data_injection.data_generator import generate_ad_impressions, generate_clicks_and_conversions, generate_bid_requests
from faker import Faker
import os
import pandas as pd


fake = Faker()
kafka_servers = ['localhost:9092']  # Update with your Kafka broker addresses
topic_ad_impressions = 'ad_impressions'
topic_clicks_conversions = 'clicks_conversions'
topic_bid_requests = 'bid_requests'
num_records = 1



def produce_data(topic, data):
    producer = KafkaProducer(bootstrap_servers=kafka_servers)
    producer.send(topic, json.dumps(data).encode('utf-8'))
    producer.flush()

st.title('AdvertiseX Data Generator')

if st.button('Generate Ad Impressions'):
    ad_impressions_data = generate_ad_impressions()
    produce_data(topic_ad_impressions, ad_impressions_data)
    st.success("Ad impressions generated successfully!")

if st.button('Generate Clicks and Conversions'):
    clicks_conversions_data = generate_clicks_and_conversions()
    produce_data(topic_clicks_conversions, clicks_conversions_data)
    st.success("Clicks and conversions generated successfully!")

if st.button('Generate Bid Requests'):
    bid_requests_data = generate_bid_requests()
    produce_data(topic_bid_requests, bid_requests_data)
    st.success("Bid requests generated successfully!")

# Function to check if a directory exists
def check_directory(directory):
    return os.path.exists(directory)

# Function to display processed data from Parquet files
def display_processed_data(directory):
    if check_directory(directory):
        st.subheader(f"Processed Data in {directory}")
        file_list = os.listdir(directory)
        parquet_files = [file for file in file_list if file.endswith(".parquet")]
        if parquet_files:
            dfs = []
            for file in parquet_files:
                parquet_path = os.path.join(directory, file)
                df = pd.read_parquet(parquet_path)
                dfs.append(df)
            combined_df = pd.concat(dfs, ignore_index=True)
            st.write(combined_df)
        else:
            st.warning("No Parquet files found in the directory.")
    else:
        st.warning(f"No processed data found in {directory}")


st.title('Processed Data Viewer')

processed_data_dir = "processed_data"

ad_impressions_dir = os.path.join(processed_data_dir, "ad_impressions")
bid_requests_dir = os.path.join(processed_data_dir, "bid_requests")
clicks_conversions_dir = os.path.join(processed_data_dir, "clicks_conversions")

if st.button('View Ad Impressions'):
    display_processed_data(ad_impressions_dir)

if st.button('View Bid Requests'):
    display_processed_data(bid_requests_dir)

if st.button('View Clicks/Conversions'):
    display_processed_data(clicks_conversions_dir)
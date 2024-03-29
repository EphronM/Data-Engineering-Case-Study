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
import re

fake = Faker()

# Kafka producer setup and other configurations...
kafka_servers = ['localhost:9092']  # Update with your Kafka broker addresses
topic_prefix = 'advertiseX'
topic_suffix = {
    'ad_impressions': 'ad_impressions',
    'clicks_conversions': 'clicks_conversions',
    'bid_requests': 'bid_requests'
}

def generate_batch_data(batch_type, num_records):
    batch_data = []
    if batch_type == 'ad_impressions':
        for _ in range(num_records):
            batch_data.append(generate_ad_impressions())
    elif batch_type == 'clicks_conversions':
        for _ in range(num_records):
            batch_data.append(generate_clicks_and_conversions())
    elif batch_type == 'bid_requests':
        for _ in range(num_records):
            batch_data.append(generate_bid_requests())
    return batch_data

def produce_batch_data(topic, batch_data):
    producer = KafkaProducer(bootstrap_servers=kafka_servers)
    for data in batch_data:
        producer.send(topic, json.dumps(data).encode('utf-8'))
    producer.flush()


def get_latest_batch_name(directory):
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


def display_processed_data():
    selected_batch_type = st.selectbox('Select Processed Data Type', ('ad_impressions', 'clicks_conversions', 'bid_requests'))
    processed_data_dir = f"Data_storage/Batch_data/{selected_batch_type}"
    
    if check_directory(processed_data_dir):
        st.subheader(f"Processed Data in {processed_data_dir}")
        file_list = os.listdir(processed_data_dir)
        directories = [file for file in file_list if os.path.isdir(os.path.join(processed_data_dir, file))]
        selected_dir = st.selectbox("Select Directory", directories)
        if st.button("View Data") and selected_dir:
            selected_dir_path = os.path.join(processed_data_dir, selected_dir)
            parquet_files = [file for file in os.listdir(selected_dir_path) if file.endswith(".parquet")]
            if parquet_files:
                dfs = []
                for file in parquet_files:
                    parquet_path = os.path.join(selected_dir_path, file)
                    df = pd.read_parquet(parquet_path)
                    dfs.append(df)
                combined_df = pd.concat(dfs, ignore_index=True)
                st.write(combined_df)
            else:
                st.warning("No Parquet files found in the selected directory.")
    else:
        st.warning(f"No processed data found in {processed_data_dir}")

def check_directory(directory):
    return os.path.exists(directory) and os.path.isdir(directory)

st.title('AdvertiseX Batch Data Generator')

batch_type = st.selectbox('Select Data Type', ('ad_impressions', 'clicks_conversions', 'bid_requests'))
num_records = st.number_input('Number of Records in a Batch', value=100, min_value=10, step=100)
#default_batch_name = get_latest_batch_name("Data_storage/Batch_data")
#batch_name = st.text_input('Batch Name', default_batch_name)

if st.button('Generate and Process Batch'):
    topic = topic_suffix[batch_type]
    batch_data = generate_batch_data(batch_type, num_records)
    produce_batch_data(topic, batch_data)
    st.success(f"{batch_type.capitalize()} batch data generated and processed successfully!")

st.title('Processed Batch Data Viewer')

display_processed_data()
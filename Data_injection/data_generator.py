import sys
import time
import json
import random
import datetime
import streamlit as st
from kafka import KafkaProducer
from faker import Faker



fake = Faker()
kafka_servers = ['localhost:9092']  # Update with your Kafka broker addresses
topic_ad_impressions = 'ad_impressions'
topic_clicks_conversions = 'clicks_conversions'
topic_bid_requests = 'bid_requests'
num_records = 1



def generate_ad_impressions():
    impression = {
        "ad_creative_id": random.randint(1, 100),
        "user_id": fake.uuid4(),
        "timestamp": fake.date_time_this_month().isoformat(),
        "website": fake.domain_name()
    }
    return impression


def generate_clicks_and_conversions():
    click_time = fake.date_time_this_month()
    conversion_time = click_time + datetime.timedelta(seconds=random.randint(1, 3600))
    click_conversion = {
        "timestamp": click_time.strftime('%Y-%m-%d %H:%M:%S'),
        "user_id": fake.uuid4(),
        "ad_campaign_id": fake.random_int(min=1, max=1000),
        "conversion_time": conversion_time.strftime('%Y-%m-%d %H:%M:%S'),
        "conversion_type": random.choice(["signup", "purchase", "download"])
    }
    return click_conversion



def generate_bid_requests():
    bid_request = {
        "user_id": fake.uuid4(),
        "auction_id": fake.uuid4(),
        "timestamp": fake.date_time_this_month().isoformat(),
        "advertiser_id": fake.random_int(min=1, max=1000),
        "ad_size": fake.random_element(elements=["300x250", "728x90", "160x600"]),
        "device_type": fake.random_element(elements=["desktop", "mobile", "tablet"]),
        "bid_amount": round(random.uniform(0.1, 10), 2)
    }
    return bid_request
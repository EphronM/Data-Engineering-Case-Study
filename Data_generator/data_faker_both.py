import os
import csv
import json
import random
import datetime
from faker import Faker
from avro import schema, datafile, io
from tqdm import tqdm
import argparse


fake = Faker()

def generate_user_ids(num_users):
    user_ids = [fake.uuid4() for _ in tqdm(range(num_users))]
    return user_ids

def generate_ad_impressions(num_records, user_ids):
    impressions = []
    for _ in tqdm(range(num_records)):
        impression = {
            "ad_creative_id": fake.random_int(min=1, max=100),  # Varying ad creative IDs
            "user_id": random.choice(user_ids),  # Reusing user IDs
            "timestamp": fake.date_time_this_month().isoformat(),  # Convert datetime to ISO format string
            "website": fake.domain_name()
        }
        impressions.append(impression)
    return impressions

def generate_clicks_and_conversions(num_records, user_ids, outputPath):
    with open(os.path.join(outputPath,"clicks_and_conversions.csv"), mode="w", newline="") as file:
        fieldnames = ["timestamp", "user_id", "ad_campaign_id", "conversion_time", "conversion_type"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        
        for _ in tqdm(range(num_records)):
            click_time = fake.date_time_this_month()
            conversion_time = click_time + datetime.timedelta(seconds=random.randint(1, 3600))  # Random conversion time within 1 hour of click
            writer.writerow({
                "timestamp": click_time.strftime('%Y-%m-%d %H:%M:%S'),
                "user_id": random.choice(user_ids),  # Reusing user IDs
                "ad_campaign_id": fake.random_int(min=1, max=1000),
                "conversion_time": conversion_time.strftime('%Y-%m-%d %H:%M:%S'),
                "conversion_type": random.choice(["signup", "purchase", "download"])  # Random conversion type
            })



def generate_bid_requests(num_records, user_ids):
    bid_requests = []
    for _ in tqdm(range(num_records)):
        bid_request = {
            "user_id": random.choice(user_ids),  # Reusing user IDs
            "auction_id": fake.uuid4(),
            "timestamp": fake.date_time_this_month().isoformat(),
            "advertiser_id": fake.random_int(min=1, max=1000),
            "ad_size": fake.random_element(elements=["300x250", "728x90", "160x600"]),
            "device_type": fake.random_element(elements=["desktop", "mobile", "tablet"]),
            "bid_amount": round(random.uniform(0.1, 10), 2)
        }
        bid_requests.append(bid_request)
    return bid_requests

if __name__ == "__main__":
    
    
    parser = argparse.ArgumentParser(description='Generate data')
    parser.add_argument('--size_count', type=int, default=5, help='Number of batchs to generate')
    parser.add_argument('--batch_size', type=int, default=100000, help='Batch size')
    args = parser.parse_args()

    num_users = 10000000  # Number of unique users
    user_ids = generate_user_ids(num_users)
    print(f"Generated {num_users} unique user IDs")

    if not os.path.exists("outputs"):
            os.mkdir("outputs")
    for batch_count in range(args.size_count):
        print("Processing Batch --> ", batch_count)
        batch_folder = f"batch_{batch_count}"
        path = os.path.join("outputs", batch_folder)
        if not os.path.exists(path):
            os.mkdir(path)

        data_counts = args.batch_size

        ad_impressions = generate_ad_impressions(data_counts, user_ids)
        with open(os.path.join("outputs", batch_folder, "ad_impressions.json"),  "w") as f:
            json.dump(ad_impressions, f, indent=4)

        print(f"Generated {data_counts} ad impressions")

        generate_clicks_and_conversions(data_counts, user_ids,os.path.join("outputs", batch_folder))

        print(f"Generated {data_counts} clicks and conversions")

        bid_requests = generate_bid_requests(data_counts, user_ids)
        schema_dict = {
            "type": "record",
            "name": "BidRequest",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "auction_id", "type": "string"},
                {"name": "timestamp", "type": "string"},
                {"name": "advertiser_id", "type": "int"},
                {"name": "ad_size", "type": "string"},
                {"name": "device_type", "type": "string"},
                {"name": "bid_amount", "type": "float"}
            ]
        }
        avro_schema = schema.parse(json.dumps(schema_dict))
        writer = datafile.DataFileWriter(open(os.path.join("outputs", batch_folder,"bid_requests.avro"), "wb"), io.DatumWriter(), avro_schema)
        for bid_request in bid_requests:
            writer.append(bid_request)
        writer.close()

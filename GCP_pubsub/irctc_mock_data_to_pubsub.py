from google.cloud import pubsub_v1
import random
import string
import uuid
import json
from datetime import datetime, timedelta

# Configuration
project_id = "project1forme-466508"
topic_id = "project1formebuckk"

def initialize_pubsub():
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)
        return publisher, topic_path
    except Exception as e:
        print(f"Failed to initialize Pub/Sub client: {e}")
        raise

# Generate mock data
def generate_mock_data(num_rows):
    try:
        data = []
        for _ in range(num_rows):
            row_key = str(uuid.uuid4())
            row_data = {
                "row_key": row_key,
                "name": ''.join(random.choices(string.ascii_letters, k=10)),
                "age": random.randint(18, 90),
                "email": ''.join(random.choices(string.ascii_lowercase, k=5)) + "@example.com",
                "join_date": (datetime.now() - timedelta(days=random.randint(0, 3650))).strftime('%Y-%m-%d'),
                "last_login": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "loyalty_points": random.randint(0, 1000),
                "account_balance": round(random.uniform(100, 10000), 2),
                "is_active": random.choice([True, False]),
                "inserted_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                "updated_at": None
            }
            data.append(row_data)
        return data
    except Exception as e:
        print(f"Failed to generate mock data: {e}")
        raise

# Publish data to Pub/Sub
def publish_to_pubsub(publisher, topic_path, data):
    try:
        for record in data:
            message_json = json.dumps(record)
            message_bytes = message_json.encode('utf-8')
            future = publisher.publish(topic_path, data=message_bytes)
            print("Data - > ",message_json)
            print(f"Published message ID: {future.result()}")
        print(f"Published {len(data)} messages successfully.")
    except Exception as e:
        print(f"Failed to publish data: {e}")
        raise

# Main execution
if __name__ == "__main__":
    try:
        publisher, topic_path = initialize_pubsub()

        # Generate and publish mock data
        mock_data = generate_mock_data(1490)
        publish_to_pubsub(publisher, topic_path, mock_data)

    except Exception as e:
        print(f"An error occurred during the execution: {e}")

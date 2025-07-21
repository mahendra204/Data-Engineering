import pandas as pd
import uuid
from google.cloud import storage
from datetime import datetime
import os

# 🔧 Configurations
bucket_name = 'mybucket1forme'                          # ✅ Update with your actual bucket name
destination_blob_name = 'mock_data/users.csv'
local_file = 'local_mockusers_data.csv'
storage.Client(project='project1forme-466508')

# 📊 Step 1: Generate mock data
def generate_mock_data(n=140):
    data = {
        'user_id': [str(uuid.uuid4()) for _ in range(n)],
        'name': [f'User_{i}' for i in range(n)],
        'email': [f'user{i}@example.com' for i in range(n)],
        'signup_date': [datetime.now().strftime('%Y-%m-%d %H:%M:%S') for _ in range(n)],
        'is_active': [i % 2 == 0 for i in range(n)],
    }
    df = pd.DataFrame(data)
    df.to_csv(local_file, index=False)
    print(f"✅ Mock data saved to {local_file}")

# ☁️ Step 2: Upload to GCP Cloud Storage
def upload_to_gcp():
    try:
        client = storage.Client()
        bucket = client.lookup_bucket(bucket_name)
        if bucket is None:
            print(f"🚫 Bucket '{bucket_name}' not found or access denied. Double-check name and permissions.")
            return
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_file)
        print(f"✅ File uploaded to gs://{bucket_name}/{destination_blob_name}")
    except Exception as e:
        print("🚫 Upload failed. Details:", e)

# 🚀 Execute both steps
generate_mock_data(600)
upload_to_gcp()

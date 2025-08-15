import pandas as pd
import random 
from google.cloud import storage

def generate_mockdata(numrows):
    employees_data = []
    try:
        for i in range(1, numrows+1):
            emp_data = {
                    "emp_id": i + 100,
                    "emp_name": random.choice(['mahi', 'ravi', 'sam']) + '' + str(i),
                    "age": random.randint(18, 60),
                    "email": str(i+100) + ''.join("@gmail.com"),
                    "salary": random.randint(25000, 100000)
                    }
            employees_data.append(emp_data)
        emp_df = pd.DataFrame(employees_data)
        local_file_name = 'employees_info.csv'
        file = emp_df.to_csv(f'{local_file_name}',index=False)
        print(f'mocke data generated and saved as local file {local_file_name}')
    except Exception as e:
        print(f"Failed to generate mock data: {e}")
#connection details
project_id = 'project1x-467415'
bucket_name = 'mybucka'
destination_file_path = 'output/employees.csv'
local_file_name = 'employees_info.csv'

def upload_to_gcp():
    try:
        client = storage.Client(project= project_id)
        bucket = client.lookup_bucket(bucket_name)
        if bucket is None:
            print(f" Bucket '{bucket_name}' not found or access denied. check whether did you provide bucket name and permissions.")
            return
        blob = bucket.blob(destination_file_path)
        blob.upload_from_filename(local_file_name)
        print(f"File uploaded to gs://{bucket_name}/{destination_file_path}")
    except Exception as e:
        print("Upload failed. Details:", e)


generate_mockdata(38900)
upload_to_gcp()
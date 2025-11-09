import json
from datetime import datetime
import pandas as pd
import numpy as np

def transform_data(element):
    try:
        # Parse the JSON message
        record = json.loads(element.replace("'", "\""))

        # Data Cleaning and Validation
        record['row_key'] = record.get('row_key', '')
        record['name'] = record.get('name', '').lower().title()  # Capitalize the name
        record['email'] = record.get('email', '').upper()  # Ensure email is uppercase
        record['is_active'] = bool(record.get('is_active', False))  # Ensure is_active is a boolean
        
        # Enriching Data
        record['loyalty_status'] = 'Gold' if record.get('loyalty_points', 0) > 500 else 'General'
        
        # Convert inserted_at and updated_at to ISO format, handle missing or invalid timestamps
        inserted_at = record.get('inserted_at')
        updated_at = record.get('updated_at')
        
        if inserted_at:
            try:
                record['inserted_at'] = datetime.strptime(inserted_at, '%Y-%m-%d %H:%M:%S').isoformat()
            except ValueError:
                record['inserted_at'] = datetime.utcnow().isoformat()
        else:
            record['inserted_at'] = datetime.utcnow().isoformat()
        
        if updated_at:
            try:
                record['updated_at'] = datetime.strptime(updated_at, '%Y-%m-%d %H:%M:%S').isoformat()
            except ValueError:
                record['updated_at'] = '1970-01-01T00:00:00'  # Set to Unix epoch if parsing fails
        else:
            record['updated_at'] = '1970-01-01T00:00:00'  # Set to Unix epoch if not provided
        
        # Calculate account age in days (assumes join_date is in YYYY-MM-DD format)
        join_date = record.get('join_date')
        if join_date:
            try:
                join_date_obj = datetime.strptime(join_date, '%Y-%m-%d')
                record['account_age_days'] = (datetime.utcnow() - join_date_obj).days
            except ValueError:
                record['account_age_days'] = 0
        else:
            record['account_age_days'] = 0  # Default to 0 if join_date is missing
        
        # Handling missing or invalid values with defaults
        record['age'] = record.get('age', 0)
        record['account_balance'] = record.get('account_balance', 0.0)
        record['loyalty_points'] = record.get('loyalty_points', 0)
        record['last_login'] = record.get('last_login', '1970-01-01T00:00:00')  # Default to epoch if missing

        # Return JSON string with double quotes
        return json.dumps(record)
    
    except Exception as e:
        print(f"Error processing record: {e}")
        return None  # Handle errors appropriately


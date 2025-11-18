import boto3                            # Imports AWS SDK for Python for interacting with AWS services
import pandas as pd                     # Imports pandas library for data manipulation

df = pd.read_csv('data/taxi_zone_lookup.csv')  # Reads taxi zone lookup CSV as a DataFrame

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')  # Sets up DynamoDB resource for 'us-east-1' region
table = dynamodb.Table('TaxiZoneLookup')          # Specifies the DynamoDB table to operate on

for idx, row in df.iterrows():                    # Iterates through each row in the DataFrame
    try:
        location_id = int(row['LocationID'])      # Attempts to convert LocationID to integer
        item = {
            'LocationID': location_id,                                 # Sets LocationID for DynamoDB item
            'Borough': str(row['Borough']) if pd.notna(row['Borough']) else "N/A",   # Handles missing Borough data
            'Zone': str(row['Zone']) if pd.notna(row['Zone']) else "N/A",            # Handles missing Zone data
            'service_zone': str(row['service_zone']) if pd.notna(row['service_zone']) else "N/A"  # Handles missing service_zone data
        }
        table.put_item(Item=item)                 # Inserts the item into DynamoDB
        print(f"Inserted LocationID {location_id}")  # Logs successful insert
    except (ValueError, TypeError):               # Handles invalid or missing LocationID values
        print(f"Skipped row {idx + 1}: LocationID is not a valid integer ({row['LocationID']})")

print("All valid taxi zones uploaded to DynamoDB!")  # Prints completion message

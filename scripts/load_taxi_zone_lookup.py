import boto3
import pandas as pd

df = pd.read_csv('data/taxi_zone_lookup.csv')

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('TaxiZoneLookup')

for idx, row in df.iterrows():
    try:
        location_id = int(row['LocationID'])
        item = {
            'LocationID': location_id,
            'Borough': str(row['Borough']) if pd.notna(row['Borough']) else "N/A",
            'Zone': str(row['Zone']) if pd.notna(row['Zone']) else "N/A",
            'service_zone': str(row['service_zone']) if pd.notna(row['service_zone']) else "N/A"
        }
        table.put_item(Item=item)
        print(f"Inserted LocationID {location_id}")
    except (ValueError, TypeError):
        print(f"Skipped row {idx + 1}: LocationID is not a valid integer ({row['LocationID']})")

print("All valid taxi zones uploaded to DynamoDB!")

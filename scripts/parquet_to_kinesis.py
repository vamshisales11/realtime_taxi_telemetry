import boto3
import pyarrow.parquet as pq
import json
import time
from datetime import datetime

# Configuration
PARQUET_FILES = [
    'data/yellow_tripdata_2024-01.parquet',
    'data/yellow_tripdata_2024-02.parquet'
]
KINESIS_STREAM_NAME = 'TaxiTripEventStream'
REGION = 'us-east-1'
BATCH_SIZE = 100  # Send in batches for efficiency
THROTTLE_DELAY = 0.1  # Delay between batches (seconds)
MAX_RECORDS_PER_FILE = 300  # LIMIT records per file

kinesis = boto3.client('kinesis', region_name=REGION)

def serialize_datetime(obj):
    """Convert datetime objects to ISO format strings for JSON serialization."""
    if isinstance(obj, (datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

# Explicit field mapping for Lambda compatibility
FIELD_ALIASES = {
    'VendorID': 'vendorid',
    'RatecodeID': 'ratecodeid',
    'PULocationID': 'pulocationid',
    'DOLocationID': 'dolocationid',
    'Airport_fee': 'airport_fee',
    # All other fields already match
}

def transform_keys(rec):
    """Map/correct field names as needed for pipeline compatibility."""
    return {FIELD_ALIASES.get(k, k): v for k, v in rec.items()}

def send_records(records):
    """Send a batch of records to Kinesis."""
    for record in records:
        try:
            # Convert datetime fields to strings
            data = json.dumps(record, default=serialize_datetime)
            kinesis.put_record(
                StreamName=KINESIS_STREAM_NAME,
                Data=data,
                PartitionKey=str(record.get("vendorid", "default"))  # Use vendorid for partitioning
            )
        except Exception as e:
            print(f"Error sending record: {e}")
            print(f"Record: {record}")

# Process each Parquet file
for parquet_file in PARQUET_FILES:
    print(f"Processing {parquet_file}...")
    table = pq.read_table(parquet_file)
    rows = table.to_pylist()
    total_records = 0

    # LIMIT records per file
    rows = rows[:MAX_RECORDS_PER_FILE]

    # Send in batches
    batch = []
    for row in rows:
        transformed_row = transform_keys(row)
        batch.append(transformed_row)
        # Debug print to verify keys (remove if not needed)
        print("[DEBUG] Transformed record keys:", list(transformed_row.keys()))
        if len(batch) >= BATCH_SIZE:
            send_records(batch)
            total_records += len(batch)
            print(f"Sent {total_records} records from {parquet_file}...")
            batch = []
            time.sleep(THROTTLE_DELAY)  # Avoid throttling

    # Send remaining records
    if batch:
        send_records(batch)
        total_records += len(batch)

    print(f"Finished {parquet_file}: Sent {total_records} records")

print("All files processed.")

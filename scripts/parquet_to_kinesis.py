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

kinesis = boto3.client('kinesis', region_name=REGION)

def serialize_datetime(obj):
    """Convert datetime objects to ISO format strings for JSON serialization."""
    if isinstance(obj, (datetime)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def send_records(records):
    """Send a batch of records to Kinesis."""
    for record in records:
        try:
            # Convert datetime fields to strings
            data = json.dumps(record, default=serialize_datetime)
            
            kinesis.put_record(
                StreamName=KINESIS_STREAM_NAME,
                Data=data,
                PartitionKey=str(record.get("VendorID", "default"))  # Use VendorID for partitioning
            )
        except Exception as e:
            print(f"Error sending record: {e}")
            print(f"Record: {record}")

# Process each Parquet file
total_records = 0
for parquet_file in PARQUET_FILES:
    print(f"Processing {parquet_file}...")
    table = pq.read_table(parquet_file)
    rows = table.to_pylist()
    
    # Send in batches
    batch = []
    for row in rows:
        batch.append(row)
        if len(batch) >= BATCH_SIZE:
            send_records(batch)
            total_records += len(batch)
            print(f"Sent {total_records} records...")
            batch = []
            time.sleep(THROTTLE_DELAY)  # Avoid throttling
    
    # Send remaining records
    if batch:
        send_records(batch)
        total_records += len(batch)
    
    print(f"Finished {parquet_file}: {len(rows)} records")

print(f"Total records sent: {total_records}")






""" How This Code Works:
1. Import Required Libraries

boto3: AWS SDK for interacting with Kinesis

pyarrow.parquet: Reads Parquet files efficiently

json: Converts Python dictionaries to JSON strings

time: Adds delays to prevent throttling

2. Configuration

PARQUET_FILES: List of local Parquet file paths to process

KINESIS_STREAM_NAME: Target Kinesis stream (must match your deployed stream)

BATCH_SIZE: Controls throttling—pauses after every 100 records

SLEEP_INTERVAL: Wait time between batches to avoid exceeding Kinesis limits

3. Read Parquet Files

pq.read_table(): Loads entire Parquet file into memory as Arrow table

.to_pylist(): Converts Arrow table to list of Python dictionaries (one per row)

4. Send Each Row to Kinesis

json.dumps(row, default=str): Converts row dict to JSON string; default=str handles datetime/decimal types

PartitionKey: Uses PULocationID to distribute records across shards (balances load)

put_record(): Sends individual record to Kinesis

5. Throttling & Error Handling

Pauses every 100 records to prevent hitting Kinesis rate limits (1000 records/sec per shard)

Catches exceptions and continues processing (doesn't crash on single bad record)

6. Progress Tracking

Prints status updates every batch and after each file completes """
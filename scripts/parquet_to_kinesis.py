import boto3
import pyarrow.parquet as pq
import json

# List your Parquet files here
PARQUET_FILES = [
    'data/yellow_tripdata_2024-01.parquet',
    'data/yellow_tripdata_2024-02.parquet'
]

KINESIS_STREAM_NAME = 'TaxiTripEventStream'
REGION = 'us-east-1'

kinesis = boto3.client('kinesis', region_name=REGION)

for parquet_file in PARQUET_FILES:
    table = pq.read_table(parquet_file)
    for row in table.to_pylist():
        kinesis.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=json.dumps(row),
            PartitionKey=str(row.get("LocationID", "default"))
        )

import json
import base64
import boto3
from datetime import datetime

# AWS clients
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
firehose = boto3.client('firehose', region_name='us-east-1')

# Resource names
ZONE_TABLE_NAME = 'TaxiZoneLookup'
BILLING_TABLE_NAME = 'TaxiMicroBilling'
FIREHOSE_STREAM_NAME = 'taxi-clean-events-stream'

zone_table = dynamodb.Table(ZONE_TABLE_NAME)
billing_table = dynamodb.Table(BILLING_TABLE_NAME)

def get_zone_name(location_id):
    """Lookup zone name from DynamoDB by LocationID."""
    try:
        response = zone_table.get_item(Key={'LocationID': int(location_id)})
        if 'Item' in response:
            return response['Item'].get('Zone', 'Unknown')
        return 'Unknown'
    except Exception as e:
        print(f"Error fetching zone for LocationID {location_id}: {e}")
        return 'Unknown'

def clean_and_validate(record):
    """Clean and validate a single trip record."""
    try:
        # Required fields check
        required_fields = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 
                          'PULocationID', 'DOLocationID', 'Total_amount']
        for field in required_fields:
            if field not in record or record[field] is None:
                return None
        
        # Data type validation and cleaning
        record['VendorID'] = int(record['VendorID'])
        record['Passenger_count'] = int(record.get('Passenger_count', 0))
        record['Trip_distance'] = float(record.get('Trip_distance', 0.0))
        record['PULocationID'] = int(record['PULocationID'])
        record['DOLocationID'] = int(record['DOLocationID'])
        record['RateCodeID'] = int(record.get('RateCodeID', 1))
        record['Payment_type'] = int(record.get('Payment_type', 0))
        
        # Financial fields
        record['Fare_amount'] = float(record.get('Fare_amount', 0.0))
        record['Extra'] = float(record.get('Extra', 0.0))
        record['MTA_tax'] = float(record.get('MTA_tax', 0.0))
        record['Improvement_surcharge'] = float(record.get('Improvement_surcharge', 0.0))
        record['Tip_amount'] = float(record.get('Tip_amount', 0.0))
        record['Tolls_amount'] = float(record.get('Tolls_amount', 0.0))
        record['Total_amount'] = float(record['Total_amount'])
        record['Congestion_Surcharge'] = float(record.get('Congestion_Surcharge', 0.0))
        record['Airport_fee'] = float(record.get('Airport_fee', 0.0))
        
        # String fields
        record['Store_and_fwd_flag'] = str(record.get('Store_and_fwd_flag', 'N'))[:1]
        
        # Business rule validation
        if record['Total_amount'] <= 0:
            return None
        if record['Trip_distance'] < 0:
            return None
            
        return record
    except Exception as e:
        print(f"Error cleaning record: {e}")
        return None

def enrich_with_zones(record):
    """Add pickup and dropoff zone names."""
    record['PUZoneName'] = get_zone_name(record['PULocationID'])
    record['DOZoneName'] = get_zone_name(record['DOLocationID'])
    return record

def store_micro_billing(record):
    """Store per-trip billing info in DynamoDB for fast lookup."""
    try:
        # Generate unique trip_id
        trip_id = f"{record['VendorID']}-{record['tpep_pickup_datetime']}-{record['PULocationID']}"
        
        billing_table.put_item(
            Item={
                'trip_id': trip_id,
                'timestamp': record['tpep_pickup_datetime'],
                'Total_amount': str(record['Total_amount']),
                'Fare_amount': str(record['Fare_amount']),
                'Tip_amount': str(record['Tip_amount']),
                'VendorID': record['VendorID'],
                'ttl': int((datetime.now().timestamp()) + (90 * 24 * 60 * 60))  # 90-day TTL
            }
        )
    except Exception as e:
        print(f"Error storing micro-billing: {e}")

def main(event, context):
    """Lambda handler triggered by Kinesis Data Stream."""
    processed_count = 0
    failed_count = 0
    
    for kinesis_record in event['Records']:
        try:
            # Decode Kinesis record
            payload = base64.b64decode(kinesis_record['kinesis']['data'])
            trip_record = json.loads(payload)
            
            # Clean and validate
            cleaned_record = clean_and_validate(trip_record)
            if cleaned_record is None:
                failed_count += 1
                continue
            
            # Enrich with zone names
            enriched_record = enrich_with_zones(cleaned_record)
            
            # Store micro-billing data
            store_micro_billing(enriched_record)
            
            # Send to Firehose #2 (which delivers to Redshift)
            firehose.put_record(
                DeliveryStreamName=FIREHOSE_STREAM_NAME,
                Record={'Data': json.dumps(enriched_record) + '\n'}
            )
            processed_count += 1
            
        except Exception as e:
            print(f"Error processing record: {e}")
            failed_count += 1
    
    print(f"Processed: {processed_count}, Failed: {failed_count}")
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': processed_count,
            'failed': failed_count
        })
    }

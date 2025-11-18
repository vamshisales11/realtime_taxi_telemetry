# --- Imports and AWS Resource Setup ---
import json
import base64
import boto3
import os
import time
from datetime import datetime

# --- CloudWatch client for custom metrics ---
cloudwatch = boto3.client('cloudwatch')

# --- Environment Variables and AWS Resource Initialization ---
REGION = os.environ.get('AWS_REGION', 'us-east-1')
ZONE_TABLE_NAME = os.environ.get('ZONE_TABLE_NAME', 'TaxiZoneLookup')
BILLING_TABLE_NAME = os.environ.get('BILLING_TABLE_NAME', 'TaxiMicroBilling')
FIREHOSE_STREAM_NAME = os.environ.get('FIREHOSE_STREAM_NAME', 'taxi-clean-events-stream')

dynamodb = boto3.resource('dynamodb', region_name=REGION)
firehose = boto3.client('firehose', region_name=REGION)
zone_table = dynamodb.Table(ZONE_TABLE_NAME)
billing_table = dynamodb.Table(BILLING_TABLE_NAME)

# --- Utility Functions (timestamp fix for Redshift) ---
def fix_timestamp(ts):
    # Converts '2024-01-01T005755' or '2024-01-01 005755' -> '2024-01-01 00:57:55' for Redshift
    try:
        ts_str = str(ts).strip()
        ts_str = ts_str.replace('T', ' ')
        parts = ts_str.split(' ')
        if len(parts) >= 2:
            ymd = parts[0]
            hms = parts[1]
            if len(hms) == 6 and ':' not in hms and hms.isdigit():
                hh = hms[0:2]
                mm = hms[2:4]
                ss = hms[4:6]
                return f"{ymd} {hh}:{mm}:{ss}"
            if ':' in hms:
                return f"{ymd} {hms}"
        return ts_str
    except Exception as e:
        print(f"[TIMESTAMP] Error fixing timestamp {ts}: {e}")
        return str(ts)

# --- Lookup Functions for Zone Names ---
def get_zone_name(location_id):
    try:
        response = zone_table.get_item(Key={'LocationID': int(location_id)})
        if 'Item' in response:
            return response['Item'].get('Zone', 'Unknown')
        return 'Unknown'
    except Exception as e:
        print(f"Error fetching zone for LocationID {location_id}: {e}")
        return 'Unknown'

# --- Cleaning and Validation Function ---
def clean_and_validate(record):
    # Validate required fields and data types
    try:
        required_fields = [
            'vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
            'passenger_count', 'trip_distance', 'pulocationid', 'dolocationid', 'total_amount'
        ]
        for field in required_fields:
            if field not in record or record[field] is None:
                print(f"[VALIDATION] Missing field {field}, id={record.get('vendorid', 'NA')}, pickup={record.get('tpep_pickup_datetime', '')}")
                # Send DeliveryFailure metric
                cloudwatch.put_metric_data(
                    Namespace='TaxiPipeline',
                    MetricData=[{
                        'MetricName': 'DeliveryFailure',
                        'Value': 1,
                        'Unit': 'Count',
                        'Dimensions': [{'Name': 'Stream', 'Value': FIREHOSE_STREAM_NAME}]
                    }]
                )
                return None
        try:
            record['vendorid'] = int(record['vendorid'])
            record['passenger_count'] = int(record.get('passenger_count', 0))
            record['trip_distance'] = float(record.get('trip_distance', 0.0))
            record['pulocationid'] = int(record['pulocationid'])
            record['dolocationid'] = int(record['dolocationid'])
            record['ratecodeid'] = int(record.get('ratecodeid', 1))
            record['payment_type'] = int(record.get('payment_type', 0))
            record['fare_amount'] = float(record.get('fare_amount', 0.0))
            record['extra'] = float(record.get('extra', 0.0))
            record['mta_tax'] = float(record.get('mta_tax', 0.0))
            record['improvement_surcharge'] = float(record.get('improvement_surcharge', 0.0))
            record['tip_amount'] = float(record.get('tip_amount', 0.0))
            record['tolls_amount'] = float(record.get('tolls_amount', 0.0))
            record['total_amount'] = float(record['total_amount'])
            record['congestion_surcharge'] = float(record.get('congestion_surcharge', 0.0))
            record['airport_fee'] = float(record.get('airport_fee', 0.0))
            record['store_and_fwd_flag'] = str(record.get('store_and_fwd_flag', 'N'))[:1]
        except Exception as e:
            print(f"[CLEANING] Type conversion error, id={record.get('vendorid', 'NA')}: {e}")
            # Send DeliveryFailure metric
            cloudwatch.put_metric_data(
                Namespace='TaxiPipeline',
                MetricData=[{
                    'MetricName': 'DeliveryFailure',
                    'Value': 1,
                    'Unit': 'Count',
                    'Dimensions': [{'Name': 'Stream', 'Value': FIREHOSE_STREAM_NAME}]
                }]
            )
            return None
        if record['total_amount'] <= 0:
            print(f"[BUSINESS RULE] Rejected for total_amount <= 0, id={record['vendorid']}")
            cloudwatch.put_metric_data(
                Namespace='TaxiPipeline',
                MetricData=[{
                    'MetricName': 'DeliveryFailure',
                    'Value': 1,
                    'Unit': 'Count',
                    'Dimensions': [{'Name': 'Stream', 'Value': FIREHOSE_STREAM_NAME}]
                }]
            )
            return None
        if record['trip_distance'] < 0:
            print(f"[BUSINESS RULE] Rejected for trip_distance < 0, id={record['vendorid']}")
            cloudwatch.put_metric_data(
                Namespace='TaxiPipeline',
                MetricData=[{
                    'MetricName': 'DeliveryFailure',
                    'Value': 1,
                    'Unit': 'Count',
                    'Dimensions': [{'Name': 'Stream', 'Value': FIREHOSE_STREAM_NAME}]
                }]
            )
            return None
        return record
    except Exception as e:
        print(f"[CLEANING] Unexpected error: {e}")
        cloudwatch.put_metric_data(
            Namespace='TaxiPipeline',
            MetricData=[{
                'MetricName': 'DeliveryFailure',
                'Value': 1,
                'Unit': 'Count',
                'Dimensions': [{'Name': 'Stream', 'Value': FIREHOSE_STREAM_NAME}]
            }]
        )
        return None

# --- Enrichment: Add Pickup and Dropoff Zone Names ---
def enrich_with_zones(record):
    # Enrich record with friendly zone names
    record['puzonename'] = get_zone_name(record['pulocationid'])
    record['dozonename'] = get_zone_name(record['dolocationid'])
    return record

# --- Microbilling Write to DynamoDB (with retries and DeliveryFailure metric) ---
def store_micro_billing(record, retries=2):
    # Store fare and trip info in micro-billing table
    for _ in range(retries + 1):
        try:
            trip_id = f"{record['vendorid']}-{record['tpep_pickup_datetime']}-{record['pulocationid']}"
            billing_table.put_item(
                Item={
                    'trip_id': trip_id,
                    'timestamp': record['tpep_pickup_datetime'],
                    'total_amount': str(record['total_amount']),
                    'fare_amount': str(record['fare_amount']),
                    'tip_amount': str(record['tip_amount']),
                    'vendorid': record['vendorid'],
                    'ttl': int((datetime.now().timestamp()) + (90 * 24 * 60 * 60))
                }
            )
            return
        except Exception as e:
            print(f"[DYNAMODB] Failed put_item, id={record.get('vendorid', 'NA')} ({_}): {e}")
            # Emit DeliveryFailure on DynamoDB error
            cloudwatch.put_metric_data(
                Namespace='TaxiPipeline',
                MetricData=[{
                    'MetricName': 'DeliveryFailure',
                    'Value': 1,
                    'Unit': 'Count',
                    'Dimensions': [{'Name': 'Stream', 'Value': FIREHOSE_STREAM_NAME}]
                }]
            )
            time.sleep(0.1)
    print(f"[DYNAMODB] All retries failed, id={record.get('vendorid', 'NA')}")

# --- Main Lambda Handler ---
def main(event, context):
    # Main entry for Lambda processing Kinesis event records
    processed_count = 0
    failed_count = 0
    records = event.get('Records', [])
    if not records:
        print("[EVENT] No Records found in event payload.")
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'No Records in event'})
        }

    for kinesis_record in records:
        trip_record = {}
        try:
            # Decode and parse Kinesis payload
            try:
                payload = base64.b64decode(kinesis_record['kinesis']['data'])
                trip_record = json.loads(payload)
            except Exception as e:
                print(f"[DECODING] base64/json error: {e} [raw_payload={kinesis_record['kinesis']['data'][:40]}...]")
                failed_count += 1
                # Emit DeliveryFailure for decode error
                cloudwatch.put_metric_data(
                    Namespace='TaxiPipeline',
                    MetricData=[{
                        'MetricName': 'DeliveryFailure',
                        'Value': 1,
                        'Unit': 'Count',
                        'Dimensions': [{'Name': 'Stream', 'Value': FIREHOSE_STREAM_NAME}]
                    }]
                )
                continue

            # Clean and validate record
            cleaned_record = clean_and_validate(trip_record)
            if cleaned_record is None:
                failed_count += 1
                continue

            # Enrich record with zones
            enriched_record = enrich_with_zones(cleaned_record)

            # Fix timestamp formats for Redshift
            for dt_field in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']:
                if dt_field in enriched_record:
                    enriched_record[dt_field] = fix_timestamp(enriched_record[dt_field])

            # Emit Data Lag metric in CloudWatch
            try:
                event_time = datetime.strptime(enriched_record['tpep_pickup_datetime'], "%Y-%m-%d %H:%M:%S")
                processed_time = datetime.utcnow()
                lag_seconds = (processed_time - event_time).total_seconds()
                cloudwatch.put_metric_data(
                    Namespace='TaxiPipeline',
                    MetricData=[{
                        'MetricName': 'DataLagSeconds',
                        'Value': lag_seconds,
                        'Unit': 'Seconds',
                        'Dimensions': [{'Name': 'Stream', 'Value': FIREHOSE_STREAM_NAME}]
                    }]
                )
            except Exception as metric_error:
                print(f"[METRIC] Error emitting DataLag metric: {metric_error}")

            # Store micro-billing info to DynamoDB
            store_micro_billing(enriched_record)

            # Attempt to send cleaned record to Firehose (up to 3 retries)
            for attempt in range(3):
                try:
                    firehose.put_record(
                        DeliveryStreamName=FIREHOSE_STREAM_NAME,
                        Record={'Data': json.dumps(enriched_record) + '\n'}
                    )
                    break
                except Exception as e:
                    print(f"[FIREHOSE] PutRecord failed ({attempt}), id={enriched_record.get('vendorid', 'NA')}: {e}")
                    cloudwatch.put_metric_data(
                        Namespace='TaxiPipeline',
                        MetricData=[{
                            'MetricName': 'DeliveryFailure',
                            'Value': 1,
                            'Unit': 'Count',
                            'Dimensions': [{'Name': 'Stream', 'Value': FIREHOSE_STREAM_NAME}]
                        }]
                    )
                    time.sleep(0.1)
            processed_count += 1
        except Exception as e:
            print(f"[HANDLER] Unexpected error: {e}, id={trip_record.get('vendorid', 'NA')}")
            failed_count += 1
            cloudwatch.put_metric_data(
                Namespace='TaxiPipeline',
                MetricData=[{
                    'MetricName': 'DeliveryFailure',
                    'Value': 1,
                    'Unit': 'Count',
                    'Dimensions': [{'Name': 'Stream', 'Value': FIREHOSE_STREAM_NAME}]
                }]
            )

    # Summary log and Lambda response
    print(f"[SUMMARY] Processed: {processed_count}, Failed: {failed_count}")
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': processed_count,
            'failed': failed_count
        })
    }



""" # --- Imports and AWS Resource Setup ---
import json
import base64
import boto3
import os
import time
from datetime import datetime

# --- Environment Variables and AWS Resource Initialization ---
REGION = os.environ.get('AWS_REGION', 'us-east-1')
ZONE_TABLE_NAME = os.environ.get('ZONE_TABLE_NAME', 'TaxiZoneLookup')
BILLING_TABLE_NAME = os.environ.get('BILLING_TABLE_NAME', 'TaxiMicroBilling')
FIREHOSE_STREAM_NAME = os.environ.get('FIREHOSE_STREAM_NAME', 'taxi-clean-events-stream')

dynamodb = boto3.resource('dynamodb', region_name=REGION)
firehose = boto3.client('firehose', region_name=REGION)
zone_table = dynamodb.Table(ZONE_TABLE_NAME)
billing_table = dynamodb.Table(BILLING_TABLE_NAME)

# --- Utility Functions ---
def fix_timestamp(ts):
    # Converts '2024-01-01T005755' or '2024-01-01 005755' -> '2024-01-01 00:57:55' for Redshift
    try:
        ts_str = str(ts).strip()
        # Replace T with space
        ts_str = ts_str.replace('T', ' ')
        
        # Split into date and time
        parts = ts_str.split(' ')
        if len(parts) >= 2:
            ymd = parts[0]
            hms = parts[1]
            
            # If time is 6 digits without colons (e.g., '005755'), add colons
            if len(hms) == 6 and ':' not in hms and hms.isdigit():
                hh = hms[0:2]
                mm = hms[2:4]
                ss = hms[4:6]
                return f"{ymd} {hh}:{mm}:{ss}"
            
            # If already has colons, return as-is
            if ':' in hms:
                return f"{ymd} {hms}"
        
        # Return original if format doesn't match
        return ts_str
    except Exception as e:
        print(f"[TIMESTAMP] Error fixing timestamp {ts}: {e}")
        return str(ts)

# --- DynamoDB Zone Lookup ---
def get_zone_name(location_id):
    try:
        response = zone_table.get_item(Key={'LocationID': int(location_id)})
        if 'Item' in response:
            return response['Item'].get('Zone', 'Unknown')
        return 'Unknown'
    except Exception as e:
        print(f"Error fetching zone for LocationID {location_id}: {e}")
        return 'Unknown'

# --- Record Cleaning and Validation ---
def clean_and_validate(record):
    try:
        required_fields = [
            'vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
            'passenger_count', 'trip_distance', 'pulocationid', 'dolocationid', 'total_amount'
        ]
        for field in required_fields:
            if field not in record or record[field] is None:
                print(f"[VALIDATION] Missing field {field}, id={record.get('vendorid', 'NA')}, pickup={record.get('tpep_pickup_datetime', '')}")
                return None
        try:
            record['vendorid'] = int(record['vendorid'])
            record['passenger_count'] = int(record.get('passenger_count', 0))
            record['trip_distance'] = float(record.get('trip_distance', 0.0))
            record['pulocationid'] = int(record['pulocationid'])
            record['dolocationid'] = int(record['dolocationid'])
            record['ratecodeid'] = int(record.get('ratecodeid', 1))
            record['payment_type'] = int(record.get('payment_type', 0))
            record['fare_amount'] = float(record.get('fare_amount', 0.0))
            record['extra'] = float(record.get('extra', 0.0))
            record['mta_tax'] = float(record.get('mta_tax', 0.0))
            record['improvement_surcharge'] = float(record.get('improvement_surcharge', 0.0))
            record['tip_amount'] = float(record.get('tip_amount', 0.0))
            record['tolls_amount'] = float(record.get('tolls_amount', 0.0))
            record['total_amount'] = float(record['total_amount'])
            record['congestion_surcharge'] = float(record.get('congestion_surcharge', 0.0))
            record['airport_fee'] = float(record.get('airport_fee', 0.0))
            record['store_and_fwd_flag'] = str(record.get('store_and_fwd_flag', 'N'))[:1]
        except Exception as e:
            print(f"[CLEANING] Type conversion error, id={record.get('vendorid', 'NA')}: {e}")
            return None
        if record['total_amount'] <= 0:
            print(f"[BUSINESS RULE] Rejected for total_amount <= 0, id={record['vendorid']}")
            return None
        if record['trip_distance'] < 0:
            print(f"[BUSINESS RULE] Rejected for trip_distance < 0, id={record['vendorid']}")
            return None
        return record
    except Exception as e:
        print(f"[CLEANING] Unexpected error: {e}")
        return None

# --- Enrichment Step: Add Zone Names ---
def enrich_with_zones(record):
    record['puzonename'] = get_zone_name(record['pulocationid'])
    record['dozonename'] = get_zone_name(record['dolocationid'])
    return record

# --- Microbilling Write to DynamoDB ---
def store_micro_billing(record, retries=2):
    for _ in range(retries + 1):
        try:
            trip_id = f"{record['vendorid']}-{record['tpep_pickup_datetime']}-{record['pulocationid']}"
            billing_table.put_item(
                Item={
                    'trip_id': trip_id,
                    'timestamp': record['tpep_pickup_datetime'],
                    'total_amount': str(record['total_amount']),
                    'fare_amount': str(record['fare_amount']),
                    'tip_amount': str(record['tip_amount']),
                    'vendorid': record['vendorid'],
                    'ttl': int((datetime.now().timestamp()) + (90 * 24 * 60 * 60))
                }
            )
            return
        except Exception as e:
            print(f"[DYNAMODB] Failed put_item, id={record.get('vendorid', 'NA')} ({_}): {e}")
            time.sleep(0.1)
    print(f"[DYNAMODB] All retries failed, id={record.get('vendorid', 'NA')}")

# --- Main Lambda Handler ---
def main(event, context):
    processed_count = 0
    failed_count = 0
    records = event.get('Records', [])
    if not records:
        print("[EVENT] No Records found in event payload.")
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'No Records in event'})
        }
    for kinesis_record in records:
        trip_record = {}
        try:
            # --- Decode and Parse Kinesis Payload ---
            try:
                payload = base64.b64decode(kinesis_record['kinesis']['data'])
                trip_record = json.loads(payload)
            except Exception as e:
                print(f"[DECODING] base64/json error: {e} [raw_payload={kinesis_record['kinesis']['data'][:40]}...]")
                failed_count += 1
                continue
            # --- Clean and Validate record ---
            cleaned_record = clean_and_validate(trip_record)
            if cleaned_record is None:
                failed_count += 1
                continue
            # --- Add Zone Enrichment ---
            enriched_record = enrich_with_zones(cleaned_record)

            # --- Fix timestamp formats for Redshift ---
            for dt_field in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']:
                if dt_field in enriched_record:
                    enriched_record[dt_field] = fix_timestamp(enriched_record[dt_field])

            # --- Store Microbilling to DynamoDB ---
            store_micro_billing(enriched_record)

            # --- Send cleaned/enriched record to Firehose for Redshift load ---
            for attempt in range(3):
                try:
                    firehose.put_record(
                        DeliveryStreamName=FIREHOSE_STREAM_NAME,
                        Record={'Data': json.dumps(enriched_record) + '\n'}
                    )
                    break
                except Exception as e:
                    print(f"[FIREHOSE] PutRecord failed ({attempt}), id={enriched_record.get('vendorid', 'NA')}: {e}")
                    time.sleep(0.1)
            processed_count += 1
        except Exception as e:
            print(f"[HANDLER] Unexpected error: {e}, id={trip_record.get('vendorid', 'NA')}")
            failed_count += 1
    print(f"[SUMMARY] Processed: {processed_count}, Failed: {failed_count}")
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': processed_count,
            'failed': failed_count
        })
    }
 """
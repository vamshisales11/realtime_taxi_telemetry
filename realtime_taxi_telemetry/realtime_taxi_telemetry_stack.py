import json
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_kinesis as kinesis,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_kinesisfirehose as firehose,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_event_sources,
    aws_redshiftserverless as redshiftserverless,
    aws_secretsmanager as secretsmanager,
)
from constructs import Construct


class RealtimeTaxiTelemetryStack(Stack):
    
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Kinesis Data Stream for ingesting trip events
        self.taxi_event_stream = kinesis.Stream(
            self,
            "TaxiEventStream",
            stream_name="TaxiTripEventStream",
            shard_count=2,
            stream_mode=kinesis.StreamMode.PROVISIONED,
            retention_period=cdk.Duration.hours(24),
        )

        # DynamoDB Taxi Zone Lookup Table
        self.zone_lookup_table = dynamodb.Table(
            self,
            "TaxiZoneLookup",
            table_name="TaxiZoneLookup",
            partition_key=dynamodb.Attribute(
                name="LocationID",
                type=dynamodb.AttributeType.NUMBER
            ),
            removal_policy=cdk.RemovalPolicy.DESTROY,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )
        
        # DynamoDB Micro-Billing Table (per-trip fare storage)
        self.micro_billing_table = dynamodb.Table(
            self,
            "TaxiMicroBillingTable",
            table_name="TaxiMicroBilling",
            partition_key=dynamodb.Attribute(
                name="trip_id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp",
                type=dynamodb.AttributeType.STRING
            ),
            removal_policy=cdk.RemovalPolicy.DESTROY,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="ttl"
        )

        # S3 bucket for raw taxi event storage
        self.taxi_event_bucket = s3.Bucket(
            self,
            "TaxiEventBucket",
            bucket_name="taxi-raw-event-persist001",
            versioned=False,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            lifecycle_rules=[
                s3.LifecycleRule(
                    transitions=[
                        s3.Transition(storage_class=s3.StorageClass.GLACIER, transition_after=cdk.Duration.days(30)),
                        s3.Transition(storage_class=s3.StorageClass.INTELLIGENT_TIERING, transition_after=cdk.Duration.days(7))
                    ],
                    expiration=cdk.Duration.days(365),
                )
            ]
        )

        # Firehose IAM role
        self.firehose_role = iam.Role(
            self,
            "FirehoseDeliveryRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
        )
        self.taxi_event_bucket.grant_write(self.firehose_role)

        # Add required permissions for Firehose to read from Kinesis stream
        self.firehose_role.add_to_policy(iam.PolicyStatement(
            actions=[
                "kinesis:DescribeStream",
                "kinesis:GetShardIterator",
                "kinesis:GetRecords",
                "kinesis:ListShards",
            ],
            resources=[self.taxi_event_stream.stream_arn]
        ))

        # Firehose delivery stream: KinesisStreamAsSource -> S3 (raw batches)
        self.raw_events_delivery_stream = firehose.CfnDeliveryStream(
            self,
            "TaxiRawEventsDeliveryStream",
            delivery_stream_name="taxi-raw-events-stream",
            delivery_stream_type="KinesisStreamAsSource",
            kinesis_stream_source_configuration=firehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
                kinesis_stream_arn=self.taxi_event_stream.stream_arn,
                role_arn=self.firehose_role.role_arn,
            ),
            s3_destination_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                bucket_arn=self.taxi_event_bucket.bucket_arn,
                role_arn=self.firehose_role.role_arn,
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    size_in_m_bs=5,
                    interval_in_seconds=60,
                ),
                compression_format="UNCOMPRESSED",
                prefix="raw_events/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/",
                error_output_prefix="raw_events/errors/"
            )
        )
        self.raw_events_delivery_stream.node.add_dependency(self.firehose_role)
        
        # Create the Lambda function
        self.enrichment_lambda = _lambda.Function(
            self,
            "TaxiEnrichmentLambda",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="lambda_handler.main",
            code=_lambda.Code.from_asset("lambda"),
            timeout=cdk.Duration.minutes(5),
        )

        # Grant Lambda read access to DynamoDB zone table
        self.zone_lookup_table.grant_read_data(self.enrichment_lambda)
        
        # Grant Lambda write access to micro-billing table
        self.micro_billing_table.grant_write_data(self.enrichment_lambda)
        
        # Grant Lambda permission to write to Firehose #2
        self.enrichment_lambda.add_to_role_policy(iam.PolicyStatement(
            actions=["firehose:PutRecord", "firehose:PutRecordBatch"],
            resources=[f"arn:aws:firehose:us-east-1:{self.account}:deliverystream/taxi-clean-events-stream"]
        ))

        # Wire Kinesis stream as Lambda event source
        lambda_event_source = lambda_event_sources.KinesisEventSource(
            stream=self.taxi_event_stream,
            starting_position=_lambda.StartingPosition.LATEST,
            batch_size=100,
        )
        self.enrichment_lambda.add_event_source(lambda_event_source)
        
        # Redshift Serverless Namespace
        self.redshift_namespace = redshiftserverless.CfnNamespace(
            self,
            "TaxiRedshiftNamespace",
            namespace_name="taxi-namespace",
            admin_username="adminuser",
            admin_user_password="AdminPass1234!"
        )

        # Redshift Serverless Workgroup
        self.redshift_workgroup = redshiftserverless.CfnWorkgroup(
            self,
            "TaxiRedshiftWorkgroup",
            workgroup_name="taxi-workgroup",
            namespace_name=self.redshift_namespace.namespace_name,
            publicly_accessible=True,
            base_capacity=8
        )
        self.redshift_workgroup.node.add_dependency(self.redshift_namespace)
        
        # Secret manager for redshift credentials
        self.redshift_creds_secret = secretsmanager.Secret(
            self,
            "RedshiftCredentialsSecret",
            secret_name="TaxiRedshiftAdminCreds",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template=json.dumps({"username": "adminuser"}),
                generate_string_key="password",
                exclude_punctuation=True,
                password_length=16,
            )
        )
        
        # Firehose Role for Redshift Delivery
        self.firehose_redshift_role = iam.Role(
            self,
            "FirehoseRedshiftDeliveryRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
        )
        self.firehose_redshift_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonRedshiftFullAccess")
        )
        self.taxi_event_bucket.grant_read(self.firehose_redshift_role)

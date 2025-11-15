import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_kinesis as kinesis,
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
            shard_count=2,  # supports ~2000 records/sec baseline
            stream_mode=kinesis.StreamMode.PROVISIONED,  # or .ON_DEMAND for auto-scaling
            retention_period=cdk.Duration.hours(24),  # Retain events for 24 hours for replay/debug
        )

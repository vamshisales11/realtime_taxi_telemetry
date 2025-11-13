import aws_cdk as core
import aws_cdk.assertions as assertions

from realtime_taxi_telemetry.realtime_taxi_telemetry_stack import RealtimeTaxiTelemetryStack

# example tests. To run these tests, uncomment this file along with the example
# resource in realtime_taxi_telemetry/realtime_taxi_telemetry_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = RealtimeTaxiTelemetryStack(app, "realtime-taxi-telemetry")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })

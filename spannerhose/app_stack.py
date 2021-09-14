import os
import sys
import subprocess
import zipfile
from base64 import b64encode
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import constructs
from aws_cdk import Duration, Fn, RemovalPolicy, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kinesisfirehose as kdf
from aws_cdk import aws_lambda as func
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from constructs import Construct


def p(*args):
    print(*args, file=sys.stderr)


def _in_mb(path: os.PathLike):
    """Given a file path, convert to humanized string "X.YZMB"

    Uses 2^20 bytes == 1 MB"""
    return f"{path.stat().st_size / 2**20:.2f}MB"


def build_deployment_zip(save_to: str):
    epoch_force = int(datetime(year=2020, month=2, day=20).timestamp())

    rust_dir = Path(__file__).parent.parent / "hose-carrier"
    p(f"Building musl-linux targeted zip for Lambda layer cwd={str(rust_dir)}")

    # install via pip to a temporary directory
    if rc := subprocess.check_call(
        "cargo build --release --target x86_64-unknown-linux-musl".split(" "),
        cwd=rust_dir,
    ):
        p(f"error: cargo build failed with exit code {rc}")
        raise Exception("Could not build asset")

    with zipfile.ZipFile(save_to, "w", compression=zipfile.ZIP_DEFLATED) as package:
        bin_path = (
            rust_dir
            / "target"
            / "x86_64-unknown-linux-musl"
            / "release"
            / "hose-carrier"
        )
        # forcing the mtime to 2020/02/20 to enable reproducible builds
        os.utime(bin_path, (epoch_force, epoch_force))
        package.write(bin_path, "extensions/hose-carrier")
    p(
        f"Finished building zipfile zipped_size={_in_mb(Path(save_to))} raw_size={_in_mb(bin_path)}"
    )
    return save_to


# TODO base an extension on this that takes OTLP export
# https://docs.aws.amazon.com/lambda/latest/dg/runtimes-extensions-api.html
# or skip extension and make an otlp-exporter library
f_code = func.InlineCode(
    """
import boto3, os
from json import dumps
kdf = boto3.client('firehose')
def handler(event, context):
    # TODO
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/firehose.html#Firehose.Client.put_record_batch
    for i in (None, 'someother'):
        event['v3'] = True
        if i:
            event['service.namespace'] = i
        kdf.put_record(
            DeliveryStreamName=os.getenv('FIREHOSE'),
            Record={
                'Data': dumps(event, default=str).encode()
            }
        )
    return {'ok': True}
""".strip()
)


class AppStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        rust_layer = func.LayerVersion(
            self,
            "HoseCarrier",
            code=func.Code.from_asset(build_deployment_zip("hose-carrier.zip")),
            compatible_runtimes=[func.Runtime.PYTHON_3_8],
            layer_version_name="hose-carrier",
        )

        dest = s3.Bucket(
            self,
            "Archive",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            lifecycle_rules=[
                s3.LifecycleRule(
                    abort_incomplete_multipart_upload_after=Duration.days(1),
                    expiration=Duration.days(3),
                    noncurrent_version_expiration=Duration.days(1),
                )
            ],
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        archive = SpanArchive(self, "Lake", dest)

        write_policy = iam.ManagedPolicy(
            self,
            "KdfWrite",
            description="Writer permissions for OTel firehose collector",
            statements=[
                iam.PolicyStatement(
                    resources=[archive.stream_arn],
                    actions=[
                        "firehose:PutRecord",
                        "firehose:PutRecordBatch",
                    ],
                )
            ],
        )
        Function(
            self,
            "WithCarrier",
            code=f_code,
            handler="index.handler",
            environment={"FIREHOSE": archive.ref, "RUST_BACKTRACE": "1"},
            managed_policies=[write_policy],
        ).function.add_layers(rust_layer)
        Function(
            self,
            "NoCarrier",
            code=f_code,
            handler="index.handler",
            environment={"FIREHOSE": archive.ref, "RUST_BACKTRACE": "1"},
            managed_policies=[write_policy],
        )


class SpanArchive(constructs.Construct):
    role: iam.Role
    stream: kdf.CfnDeliveryStream

    @property
    def ref(self):
        return self.stream.ref

    @property
    def stream_arn(self):
        return self.stream.attr_arn

    def __init__(
        self, scope: Construct, construct_id: str, dest: s3.Bucket, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.role = iam.Role(
            self,
            "KdfDeliveryRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
            inline_policies=[
                iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["s3:PutObject"],
                            resources=[dest.arn_for_objects("*")],
                        ),
                    ]
                )
            ],
        )
        self.stream = kdf.CfnDeliveryStream(
            self,
            "Stream",
            delivery_stream_type="DirectPut",
            extended_s3_destination_configuration={
                "bucketArn": dest.bucket_arn,
                "roleArn": self.role.role_arn,
                "compressionFormat": "UNCOMPRESSED",
                # namespace can't be projected
                # but date can https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html
                # https://radar.io/blog/custom-partitions-with-kinesis-and-athena
                "prefix": "traces/namespace=!{partitionKeyFromQuery:ns}/date=!{timestamp:yyyy-MM-dd}/",
                "errorOutputPrefix": "errors/date=!{timestamp:yyyy-MM-dd}/hour=!{timestamp:HH}/!{firehose:error-output-type}/",
                "processingConfiguration": {
                    "enabled": True,
                    "processors": [
                        {"type": "AppendDelimiterToRecord"},
                        {
                            "type": "RecordDeAggregation",
                            "parameters": [
                                {
                                    "parameterName": "SubRecordType",
                                    "parameterValue": "DELIMITED",
                                },
                                {
                                    "parameterName": "Delimiter",
                                    "parameterValue": b64encode(b"||").decode(),
                                },
                            ],
                        },
                        {
                            "type": "MetadataExtraction",
                            "parameters": [
                                {
                                    "parameterName": "MetadataExtractionQuery",
                                    "parameterValue": """{ns:(."service.namespace"//"nil")}""",
                                },
                                {
                                    "parameterName": "JsonParsingEngine",
                                    "parameterValue": "JQ-1.6",
                                },
                            ],
                        },
                    ],
                },
            },
        )
        self.stream.add_property_override(
            "ExtendedS3DestinationConfiguration.DynamicPartitioningConfiguration.Enabled",
            True,
        )


class Function(constructs.Construct):
    log_group: Optional[logs.LogGroup]
    role: iam.Role
    function: func.Function

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        code: func.Code,
        handler: str,
        managed_policies: List[iam.IManagedPolicy],
        environment: Dict[str, Any],
        allow_logging: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.role = iam.Role(
            self,
            "Role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=managed_policies,
        )
        self.function = func.Function(
            self,
            "Func",
            handler=handler,
            runtime=func.Runtime.PYTHON_3_8,
            code=code,
            environment=environment,
            role=self.role,
        )

        if allow_logging:
            self.log_group = logs.LogGroup(
                self,
                "Logs",
                log_group_name=f"/aws/lambda/{self.function.function_name}",
                removal_policy=RemovalPolicy.DESTROY,
                retention=logs.RetentionDays.THREE_DAYS,
            )
            self.role.add_to_principal_policy(
                iam.PolicyStatement(
                    actions=[
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                    ],
                    resources=[
                        f"arn:{Fn.ref('AWS::Partition')}:logs:{Fn.ref('AWS::Region')}:{Fn.ref('AWS::AccountId')}:log-group:/aws/lambda/{Fn.ref('AWS::StackName')}-{construct_id}F*",
                    ],
                )
            )

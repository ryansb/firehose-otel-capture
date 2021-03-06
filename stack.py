#!/usr/bin/env python3
import os
import subprocess
import sys
import zipfile
from base64 import b64encode
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import aws_cdk as cdk
from aws_cdk import Duration, Fn, RemovalPolicy, Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kinesisfirehose as kdf
from aws_cdk import aws_lambda as func
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from constructs import Construct


p = lambda *a: print(*a, file=sys.stderr)


def _mb_size(path: os.PathLike):
    """Given a file path, convert to humanized string "X.YZMB"

    Uses 2^20 bytes == 1 MB"""
    return f"{path.stat().st_size / 2**20:.2f}MB"


def rust_compile_to_zip(save_to: str):
    epoch_force = int(datetime(year=2020, month=2, day=20).timestamp())

    rust_dir = Path(__file__).parent / "hose-carrier"
    p(f"Building musl-linux targeted zip for Lambda layer cwd={str(rust_dir)}")

    # install via pip to a temporary directory
    if rc := subprocess.check_call(
        "cargo build --release --target x86_64-unknown-linux-musl".split(" "),
        cwd=rust_dir,
    ):
        p(f"error: cargo build failed with exit code {rc}")
        raise Exception("Could not build asset")

    bin_path = (
        rust_dir / "target" / "x86_64-unknown-linux-musl" / "release" / "hose-carrier"
    )

    try:
        if rc := subprocess.check_call(
            ["upx", bin_path],
            cwd=rust_dir,
        ):
            p(f"error: Could not run upx. Exit code {rc}")
            raise Exception("Could not build asset")
    except FileNotFoundError:
        p("warn: upx not installed, skipping binary compression")

    with zipfile.ZipFile(save_to, "w", compression=zipfile.ZIP_DEFLATED) as package:
        # forcing the mtime to 2020/02/20 to enable reproducible builds
        os.utime(bin_path, (epoch_force, epoch_force))
        package.write(bin_path, "extensions/hose-carrier")

    p(
        f"Layer asset: zipped_size={_mb_size(Path(save_to))} raw_size={_mb_size(bin_path)}"
    )
    return save_to


# TODO base an extension on this that takes OTLP export
# https://docs.aws.amazon.com/lambda/latest/dg/runtimes-extensions-api.html
# or skip extension and make an otlp-exporter library
loc_code = func.InlineCode(
    """
import http.client
from json import dumps
def handler(event, context):
    client = http.client.HTTPConnection('localhost:3030')
    client.request(
        "POST",
        "/hose",
        dumps(dict(
            aws_request_id=context.aws_request_id,
            aws_lambda_arn=context.invoked_function_arn,
            **event,
        ), default=str).encode()
    )
    client.getresponse().read().decode()
    event['long'] = 'a' * 140
    for i in (None, 'someother', 'athird'):
        event['v3'] = True
        if i:
            event['service.namespace'] = i
        client.request("POST", "/hose", dumps(event, default=str).encode())
        client.getresponse().read().decode()
    return {'ok': True}
""".strip()
)


class AppStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        rust_layer = func.LayerVersion(
            self,
            "HoseCarrier",
            code=func.Code.from_asset(rust_compile_to_zip("hose-carrier.zip")),
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
                    expiration=Duration.days(1),
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
            code=loc_code,
            handler="index.handler",
            environment={"HOSE_CARRIER_TARGET": archive.ref},
            managed_policies=[write_policy],
        ).function.add_layers(rust_layer)


class SpanArchive(Construct):
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
                "bufferingHints": {
                    "intervalInSeconds": 60,
                },
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
                                    "parameterValue": b64encode(b"\n").decode(),
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


class Function(Construct):
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
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.role = iam.Role(
            self,
            "Role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            inline_policies=[
                iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                            resources=[
                                f"arn:{Fn.ref('AWS::Partition')}:logs:{Fn.ref('AWS::Region')}:{Fn.ref('AWS::AccountId')}:log-group:/aws/lambda/{Fn.ref('AWS::StackName')}-{construct_id}F*",
                            ],
                        )
                    ]
                )
            ],
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

        self.log_group = logs.LogGroup(
            self,
            "Logs",
            log_group_name=f"/aws/lambda/{self.function.function_name}",
            removal_policy=RemovalPolicy.DESTROY,
            retention=logs.RetentionDays.THREE_DAYS,
        )


app = cdk.App()
AppStack(app, "SpannerHose")
app.synth()

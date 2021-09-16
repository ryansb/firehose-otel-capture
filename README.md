# firehose-otel-capture

`spannerhose` is an AWS Kinesis Firehose collector for opentelemetry or other JSON events

This project uses Firehose's new [Dynamic Partitioning](https://aws.amazon.com/blogs/big-data/kinesis-data-firehose-now-supports-dynamic-partitioning-to-amazon-s3/) to partition incoming JSON events by the OpenTelemetry `service.namespace` field. Bucket contents are partitioned using custom [S3 prefixes](https://docs.aws.amazon.com/firehose/latest/dev/s3-prefixes.html):

```
traces/namespace=!{partitionKeyFromQuery:ns}/date=!{timestamp:yyyy-MM-dd}/

ns = ."service.namespace" OR "nil"
timestamp = current UTC time, as YYYY-MM-DD
```

Sample path: `traces/namespace=petstore/date=2021-02-01/SpannerHose-LakeStreamFF47BEAA-jUKyOeLdxYqd-1-2021-02-01-15-22-36-d49a6ead-b4fc-315e-b7c7-7590ff6bb336`

## Usage

`pipenv run synth` - build the rust sidecar and CDK stack

`pipenv run deploy` - deploy the `spannerhose` stack to your local AWS account/region with environment credentials

`cd hose-carrier ; ./rush` - live compiler session for working on the sidecar

## Thanks

This project wouldn't be around without the AWS Labs cross-compilation docs in [aws-lambda-rust-runtime](https://github.com/awslabs/aws-lambda-rust-runtime), Duarte's [Building a Lambda Extension in Rust](https://dev.to/aws-builders/building-an-aws-lambda-extension-with-rust-3p81) tutorial, and the [aws-sdk-rs](https://github.com/awslabs/aws-sdk-rust) team.
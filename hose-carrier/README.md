# hose-carrier

`hose-carrier` is a forwarder for Kinesis Data Firehose that runs as a Lambda
extension and buffers newline-delimited JSON messages up to 5KB.

## Usage

Only firehoses in the current region are supported, provide the name as the
Lambda env var `HOSE_CARRIER_TARGET`.

`POST` JSON messages to `http://localhost:3030/hose` from your app. A `200 OK`
response means the message is buffered locally, and `hose-carrier` will flush
the buffer during the `SHUTDOWN` phase of the function lifecycle.

### Sample Python function

```python
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
    return {'ok': True}
```

## Build Hacks

To reduce cold start and package size, this repo adds build optimizations from
[min-sized-rust](https://github.com/johnthagen/min-sized-rust) when doing the
`generic-musl` cross-compile that creates the layer. See the `profile.release`
section of `Cargo.toml`. Normal panic behavior is still enabled and
`RUST_BACKTRACE=1` will still work to get trace info from Lambda functions using
this extension.

The rust binary is then repacked with [upx](https://github.com/upx/upx) if `upx`
is in the path.
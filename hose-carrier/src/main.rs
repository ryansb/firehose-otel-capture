use anyhow::{ensure, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_firehose::model::Record;
use aws_sdk_firehose::Client as FirehoseClient;
use aws_sdk_firehose::{Blob, Error, Region, PKG_VERSION};
use futures::executor::block_on;
use reqwest::blocking::Client;
use reqwest::StatusCode;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::time::SystemTime;
use tokio::sync::{mpsc, oneshot};
use warp::reject;
use warp::{Buf, Filter};

const EXTENSION_NAME: &str = "hose-carrier";
const EXTENSION_NAME_HEADER: &str = "Lambda-Extension-Name";
const EXTENSION_ID_HEADER: &str = "Lambda-Extension-Identifier";

fn base_url() -> Result<String, env::VarError> {
    Ok(format!(
        "http://{}/2020-01-01/extension",
        env::var("AWS_LAMBDA_RUNTIME_API")?
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Tracing {
    pub r#type: String,
    pub value: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE", tag = "eventType")]
enum NextEventResponse {
    #[serde(rename_all = "camelCase")]
    Invoke {
        deadline_ms: u64,
        request_id: String,
        invoked_function_arn: String,
        tracing: Tracing,
    },
    #[serde(rename_all = "camelCase")]
    Shutdown {
        shutdown_reason: String,
        deadline_ms: u64,
    },
}

fn next_event(client: &reqwest::blocking::Client, ext_id: &str) -> Result<NextEventResponse> {
    let url = format!("{}/event/next", base_url()?);
    Ok(client
        .get(&url)
        .header(EXTENSION_ID_HEADER, ext_id)
        .send()?
        .json()?)
}

#[derive(Debug)]
struct RegisterResponse {
    pub extension_id: String,
}

fn register_as_extension(client: &reqwest::blocking::Client) -> Result<RegisterResponse> {
    let mut map = HashMap::new();
    map.insert("events", vec!["INVOKE", "SHUTDOWN"]);
    let res = client
        .post(&format!("{}/register", base_url()?))
        .header(EXTENSION_NAME_HEADER, EXTENSION_NAME)
        .json(&map)
        .send()?;

    ensure!(
        res.status() == StatusCode::OK,
        "Unable to register extension",
    );

    let ext_id = res.headers().get(EXTENSION_ID_HEADER).unwrap().to_str()?;

    Ok(RegisterResponse {
        extension_id: ext_id.into(),
    })
}

async fn message_forwarding_worker(mut incoming: mpsc::Receiver<String>) {
    let mut buf: Vec<u8> = Vec::new();

    loop {
        match incoming.recv().await {
            Some(data) => {
                if !buf.is_empty() {
                    buf.extend(String::from("\n").as_str().as_bytes().to_vec());
                }
                buf.extend(data.as_str().as_bytes().to_vec());
                if buf.len() > 1024 * 5 {
                    put_record(Vec::clone(&buf)).await;
                    buf.clear();
                }
            }
            None => {
                if !buf.is_empty() {
                    put_record(Vec::clone(&buf)).await;
                }
                return;
            }
        }
    }
}

async fn put_record(data: Vec<u8>) {
    let (hose, client) = client_from_env().await;
    for _ in 1..4 {
        let l = data.clone();
        match client
            .put_record()
            .delivery_stream_name(&hose)
            .record(Record::builder().data(Blob::new(l)).build())
            .send()
            .await
        {
            Err(err) => {
                eprintln!("HoseCarrier: Retrying in put_record: {:?}", err);
            }
            Ok(passed) => {
                println!("HoseCarrier: Sent record_id={}", passed.record_id.unwrap());
                return;
            }
        }
    }
    println!("HoseCarrier: Ran out of retries");
}

async fn client_from_env() -> (String, FirehoseClient) {
    // create the Firehose client based on the env vars for AWS_REGION, HOSE_CARRIER_TARGET, and AWS_ creds
    let region_provider =
        RegionProviderChain::first_try(env::var("AWS_REGION").ok().map(Region::new))
            .or_default_provider()
            .or_else(Region::new("us-west-2"));

    let shared_config = aws_config::from_env().region(region_provider).load().await;

    let hose = env::var("HOSE_CARRIER_TARGET").ok().unwrap();
    println!(
        "HoseCarrier: FirehoseSdk=v{} Region={} FirehoseName={}",
        PKG_VERSION,
        shared_config.region().unwrap(),
        hose
    );
    (hose, FirehoseClient::new(&shared_config))
}

async fn lifecycle_event_loop(transmit_queue: mpsc::Sender<String>) -> Result<()> {
    let client = Client::builder().timeout(None).build()?;
    let r = register_as_extension(&client)?;
    loop {
        let evt = next_event(&client, &r.extension_id);

        match evt {
            Ok(evt) => match evt {
                NextEventResponse::Invoke {
                    request_id,
                    invoked_function_arn,
                    ..
                } => {
                    if let Err(err) = transmit_queue
                        .send(format!(
                            // very lazy JSON
                            "{{\"service.namespace\":\"aws-lambda-invoke\", \"now-epoch-ms\":{}, \"aws-request-id\":\"{}\", \"function-arn\":\"{}\"}}",
                            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?.as_millis(), request_id, invoked_function_arn
                        ))
                        .await
                    {
                        eprintln!(
                            "HoseCarrier: Could not send string to transmit_queue: {:?}",
                            err
                        )
                    };
                }
                NextEventResponse::Shutdown {
                    shutdown_reason, ..
                } => {
                    println!("HoseCarrier: Exiting: {}", shutdown_reason);
                    return Ok(());
                }
            },
            Err(err) => {
                eprintln!("HoseCarrier: Error: {:?}", err);
                println!("HoseCarrier: Exiting after error");
                return Err(err);
            }
        }
    }
}

async fn stringify_body(mut body: impl Buf) -> Result<String, warp::Rejection> {
    // It could have several non-contiguous slices of memory...
    let mut out = String::from("");
    while body.has_remaining() {
        let cnt = body.chunk().len();
        let w = String::from_utf8_lossy(body.chunk());
        out.push_str(&w);
        body.advance(cnt);
    }
    if !out.is_empty() {
        Ok(out)
    } else {
        Err(reject::not_found())
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let (tx, rx) = mpsc::channel::<String>(10);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let forwarder_handle = tokio::spawn(message_forwarding_worker(rx));

    let web_tx = tx.clone(); // channel writer for the web listener
    let routes = warp::path("hose")
        .and(warp::post())
        .and(warp::body::content_length_limit(1024 * 1000)) // 1000KiB kinesis firehose message limit
        .and(warp::body::aggregate()) // handle multiparted requests
        .and_then(stringify_body) // converts the byte-body to string https://docs.rs/warp/0.3.1/warp/reject/index.html
        .map(move |body: String| {
            if let Err(e) = block_on(web_tx.send(body)) {
                eprintln!(
                    "HoseCarrier: Failed to queue body to message_forwarding_worker: {}",
                    e
                )
            };

            String::from("OK\n")
        });

    tokio::spawn(
        warp::serve(routes)
            .bind_with_graceful_shutdown(([127, 0, 0, 1], 3030), async {
                shutdown_rx.await.ok();
            })
            .1, // serve gives (Address, Server) tuple back, we just want to spawn the server
    );

    if let Err(error) = lifecycle_event_loop(tx).await {
        panic!(
            "HoseCarrier: Something went wrong with the main waiter loop {:?}",
            error
        )
    };

    let _ = shutdown_tx.send(()); // shut down HTTP server
    forwarder_handle.await.unwrap(); // wait to wrap up firehose sends

    Ok(())
}

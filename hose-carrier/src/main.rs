use anyhow::{ensure, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_firehose::model::Record;
use aws_sdk_firehose::Client as FirehoseClient;
use aws_sdk_firehose::{Blob, Error, Region, PKG_VERSION};
use reqwest::blocking::Client;
use reqwest::StatusCode;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use tokio::sync::mpsc;

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

fn register(client: &reqwest::blocking::Client) -> Result<RegisterResponse> {
    let mut map = HashMap::new();
    map.insert("events", vec!["INVOKE", "SHUTDOWN"]);
    let url = format!("{}/register", base_url()?);
    println!("Got URL {}", url);
    let res = client
        .post(&url)
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
    let (hose, client) = build_client().await;
    loop {
        match incoming.recv().await {
            None => break,
            Some(data) => {
                match client
                    .put_record()
                    .delivery_stream_name(&hose)
                    .record(
                        Record::builder()
                            .data(Blob::new(data.as_str().as_bytes()))
                            .build(),
                    )
                    .send()
                    .await
                {
                    Err(err) => {
                        eprintln!("Error: {:?}", err);
                        println!("Exiting after error");
                    }
                    Ok(_) => {
                        println!("Sent kinesis msg");
                    }
                }
            }
        }
    }
}

async fn build_client() -> (String, FirehoseClient) {
    // create the Firehose client based on the env vars for AWS_REGION, FIREHOSE, and AWS_ creds
    let region_provider =
        RegionProviderChain::first_try(env::var("AWS_REGION").ok().map(Region::new))
            .or_default_provider()
            .or_else(Region::new("us-west-2"));

    let shared_config = aws_config::from_env().region(region_provider).load().await;

    let hose = env::var("FIREHOSE")
        .ok()
        .or_else(|| Some(String::from("SpannerHose-LakeStreamFF47BEAA-jUKyOeLdxYqd")))
        .unwrap();
    println!(
        "PKG_VERSION={} Region={} FirehoseName={}",
        PKG_VERSION,
        shared_config.region().unwrap(),
        hose
    );
    (hose, FirehoseClient::new(&shared_config))
}

async fn handle_lambda_events(transmit_queue: mpsc::Sender<String>) -> Result<()> {
    println!("Loaded bin...");
    let client = Client::builder().timeout(None).build()?;
    let r = register(&client)?;
    loop {
        println!("Waiting for event...");
        let evt = next_event(&client, &r.extension_id);

        match evt {
            Ok(evt) => match evt {
                NextEventResponse::Invoke {
                    request_id,
                    deadline_ms,
                    invoked_function_arn,
                    ..
                } => {
                    let s = format!(
                        // very lazy JSON
                        "{{\"request\":\"{}\", \"function_arn\":\"{}\"}}",
                        request_id, invoked_function_arn
                    );
                    match transmit_queue.send(s).await {
                        Ok(_) => {}
                        Err(err) => {
                            eprintln!("Could not send string to transmit_queue: {:?}", err)
                        }
                    };
                    println!("Start event {}; deadline: {}", request_id, deadline_ms);
                }
                NextEventResponse::Shutdown {
                    shutdown_reason, ..
                } => {
                    println!("Exiting: {}", shutdown_reason);
                    return Ok(());
                }
            },
            Err(err) => {
                eprintln!("Error: {:?}", err);
                println!("Exiting after error");
                return Err(err);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let (tx, rx) = mpsc::channel::<String>(5);
    let forwarder_handle = tokio::spawn(message_forwarding_worker(rx));

    println!("Loaded and sent liveness message...");
    match handle_lambda_events(tx).await {
        Err(error) => panic!("Something went wrong with the main waiter loop {:?}", error),
        Ok(_) => println!("Done waiting around"),
    };

    forwarder_handle.await.unwrap();

    Ok(())
}

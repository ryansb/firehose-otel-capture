use anyhow::{ensure, Result};
use reqwest::blocking::Client;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::BufReader;
use std::time;

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

#[derive(Deserialize)]
struct InvocationResult {
    payload: Value,
}

fn read_result(req_id: String) -> Result<InvocationResult> {
    let filename = format!("/tmp/{}", req_id);
    let f = fs::File::open(filename)?;
    let reader = BufReader::new(f);
    let res = serde_json::from_reader(reader)?;
    Ok(res)
}

fn process_result(req_id: String) {
    match read_result(req_id) {
        Ok(InvocationResult { payload }) => println!("Payload: {}", payload),
        Err(e) => eprintln!("Error processing invocation result: {:?}", e),
    }
}

fn main() -> Result<()> {
    println!("Loaded bin...");
    let client = Client::builder().timeout(None).build()?;
    let r = register(&client)?;
    let mut prev_request: Option<String> = Option::None;
    loop {
        std::thread::sleep(time::Duration::from_secs(1));
        println!("Waiting for event...");
        let evt = next_event(&client, &r.extension_id);

        prev_request.map(process_result);

        match evt {
            Ok(evt) => match evt {
                NextEventResponse::Invoke {
                    request_id,
                    deadline_ms,
                    ..
                } => {
                    println!("Start event {}; deadline: {}", request_id, deadline_ms);
                    prev_request = Some(request_id);
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

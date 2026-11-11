//! Subscribe to all slot statuses (`interslot_updates`) on several yellowstone-grpc
//! servers and write every update into one CSV, for a fixed duration.
//! `recv_ts_us` is the local wall clock on receipt, `created_at_us` the server's stamp;
//! comparing the two includes clock offset between the hosts.
//!
//! ```sh
//! cargo run -p shipstern-example-slot-dump -- \
//!     --endpoint https://a.example:10000 --x-token TOKEN_A \
//!     --endpoint https://b.example:10000 --x-token TOKEN_B \
//!     --duration-secs 180 --out slots.csv
//! ```

use std::{
    collections::HashMap,
    fs::File,
    io::{BufWriter, Write},
    path::PathBuf,
    process::ExitCode,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clap::Parser;
use futures_util::StreamExt;
use tokio::{sync::mpsc, task::JoinSet};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::{
    geyser::{
        subscribe_update::UpdateOneof, SlotStatus, SubscribeRequest, SubscribeRequestFilterSlots,
    },
    tonic::transport::ClientTlsConfig,
};

const CSV_HEADER: &str = "node_id,recv_ts_us,created_at_us,slot,bank_id,parent,status,dead_error";

#[derive(Parser)]
struct Args {
    /// yellowstone-grpc endpoint; repeat per server. The node_id is its position, starting at 0.
    #[arg(long, required = true)]
    endpoint: Vec<String>,

    /// x-token per endpoint, matched by position; repeat once per `--endpoint` or omit.
    /// Pass "" for an endpoint without a token.
    #[arg(long)]
    x_token: Vec<String>,

    #[arg(long, default_value_t = 180)]
    duration_secs: u64,

    #[arg(long, default_value = "slots.csv")]
    out: PathBuf,
}

struct Row {
    node_id: usize,
    recv_ts_us: u128,
    created_at_us: Option<i64>,
    slot: u64,
    bank_id: Option<u64>,
    parent: Option<u64>,
    /// Raw enum value, so a status this proto version does not know still shows up.
    status: i32,
    dead_error: Option<String>,
}

#[tokio::main]
async fn main() -> ExitCode {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let args = Args::parse();

    if !args.x_token.is_empty() && args.x_token.len() != args.endpoint.len() {
        eprintln!("pass one --x-token per --endpoint, or none");
        return ExitCode::FAILURE;
    }

    let mut out = BufWriter::new(File::create(&args.out).expect("create output file"));
    writeln!(out, "{CSV_HEADER}").expect("write CSV header");

    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut streams = JoinSet::new();

    for (node_id, endpoint) in args.endpoint.iter().enumerate() {
        let x_token = args.x_token.get(node_id).filter(|t| !t.is_empty()).cloned();

        eprintln!("node_id {node_id} = {endpoint}");
        streams.spawn(stream_slots(node_id, endpoint.clone(), x_token, tx.clone()));
    }
    drop(tx);

    let deadline = tokio::time::sleep(Duration::from_secs(args.duration_secs));
    tokio::pin!(deadline);

    let mut rows = 0u64;
    let mut complete = true;

    loop {
        tokio::select! {
            () = &mut deadline => break,
            // A node that stops early leaves a one-sided capture; its task already said why.
            Some(_) = streams.join_next() => {
                complete = false;
                break;
            }
            Some(row) = rx.recv() => {
                write_row(&mut out, &row).expect("write CSV row");
                rows += 1;
            }
        }
    }

    // Write what is already queued, then stop the streams.
    rx.close();
    while let Some(row) = rx.recv().await {
        write_row(&mut out, &row).expect("write CSV row");
        rows += 1;
    }
    streams.abort_all();

    out.flush().expect("flush CSV");
    eprintln!("wrote {rows} rows to {}", args.out.display());

    if !complete {
        eprintln!("a stream ended before the deadline; the capture is incomplete");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

/// Stream slot updates from one server into `tx` until the stream ends or the receiver is gone.
async fn stream_slots(
    node_id: usize,
    endpoint: String,
    x_token: Option<String>,
    tx: mpsc::UnboundedSender<Row>,
) {
    let result = async {
        // Keepalive surfaces a silently dropped connection as a stream error.
        let mut client = GeyserGrpcClient::build_from_shared(endpoint)?
            .x_token(x_token)?
            .connect_timeout(Duration::from_secs(10))
            .http2_keep_alive_interval(Duration::from_secs(10))
            .keep_alive_timeout(Duration::from_secs(5))
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .connect()
            .await?;

        let request = SubscribeRequest {
            slots: HashMap::from([(
                "slots".to_owned(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(false),
                    interslot_updates: Some(true),
                },
            )]),
            ..Default::default()
        };

        // Hold the sink: older servers end the subscription when the request stream closes.
        let (_sink, mut stream) = client.subscribe_with_request(Some(request)).await?;

        while let Some(update) = stream.next().await {
            let update = update?;
            let recv_ts_us = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_micros();

            let Some(UpdateOneof::Slot(slot)) = update.update_oneof else {
                continue;
            };

            let row = Row {
                node_id,
                recv_ts_us,
                created_at_us: update
                    .created_at
                    .map(|ts| ts.seconds * 1_000_000 + i64::from(ts.nanos) / 1_000),
                slot: slot.slot,
                bank_id: slot.bank_id,
                parent: slot.parent,
                status: slot.status,
                dead_error: slot.dead_error,
            };

            if tx.send(row).is_err() {
                return Ok(());
            }
        }

        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;

    match result {
        Ok(()) => eprintln!("node_id {node_id}: stream ended"),
        Err(err) => eprintln!("node_id {node_id}: stream failed: {err}"),
    }
}

/// Example output:
/// ```text
/// 0,1762862400123456,1762862400120001,380012345,7,380012344,SLOT_CREATED_BANK,
/// 1,1762862400124001,,380012345,,,SLOT_FIRST_SHRED_RECEIVED,
/// ```
fn write_row(out: &mut impl Write, row: &Row) -> std::io::Result<()> {
    let status = SlotStatus::try_from(row.status)
        .map(|s| s.as_str_name().to_owned())
        .unwrap_or_else(|_| row.status.to_string());

    // Quoted per RFC 4180, since the error text can contain commas.
    let dead_error = row
        .dead_error
        .as_deref()
        .map(|e| format!("\"{}\"", e.replace('"', "\"\"")))
        .unwrap_or_default();

    writeln!(
        out,
        "{},{},{},{},{},{},{status},{dead_error}",
        row.node_id,
        row.recv_ts_us,
        opt(row.created_at_us),
        row.slot,
        opt(row.bank_id),
        opt(row.parent),
    )
}

fn opt(v: Option<impl ToString>) -> String {
    v.map(|v| v.to_string()).unwrap_or_default()
}

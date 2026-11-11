//! Subscribe on several yellowstone-grpc servers to all slot statuses (`interslot_updates`),
//! JUP6 transactions and cbBTC mint account updates, and write them into one CSV.
//! `created_at_us` is the server's stamp; comparing nodes includes their clock offset.
//!
//! ```sh
//! cargo run -p shipstern-example-slot-dump -- \
//!     --endpoint https://a.example:10000 --x-token TOKEN_A \
//!     --endpoint https://b.example:10000 --x-token TOKEN_B \
//!     --duration-secs 180 --out slots.csv
//! ```

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    path::PathBuf,
    process::ExitCode,
    time::{Duration, SystemTime},
};

use clap::Parser;
use futures_util::StreamExt;
use tokio::{sync::mpsc, task::JoinSet};
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::{
    geyser::{
        subscribe_update::UpdateOneof, SlotStatus, SubscribeRequest,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions,
    },
    tonic::transport::ClientTlsConfig,
};

const JUP6_PROGRAM: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const TOKEN2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

const CSV_HEADER: &str = "type,slot,bank_id,status,created_at_us,node_id,key,index";

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
    created_at_us: Option<i64>,
    slot: u64,
    bank_id: Option<u64>,
    kind: RowKind,
}

enum RowKind {
    /// Raw enum value, so a status this proto version does not know still shows up.
    Slot {
        status: i32,
    },
    Tx {
        signature: Vec<u8>,
        index: u64,
    },
    Account {
        pubkey: Vec<u8>,
        write_version: u64,
    },
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

    // Each stream appends its server's version line, e.g. `dump.csv.txt`.
    let versions_path = PathBuf::from({
        let mut path = args.out.clone().into_os_string();
        path.push(".txt");
        path
    });
    let mut versions = File::create(&versions_path).expect("create versions file");
    writeln!(
        versions,
        "start {}",
        humantime::format_rfc3339_millis(SystemTime::now())
    )
    .expect("write start time");
    drop(versions);

    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut streams = JoinSet::new();

    for (node_id, endpoint) in args.endpoint.iter().enumerate() {
        let x_token = args.x_token.get(node_id).filter(|t| !t.is_empty()).cloned();

        eprintln!("node_id {node_id} = {endpoint}");
        streams.spawn(stream_updates(
            node_id,
            endpoint.clone(),
            x_token,
            versions_path.clone(),
            tx.clone(),
        ));
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

    // Append, so the node lines written in between are kept.
    let mut versions = OpenOptions::new()
        .append(true)
        .open(&versions_path)
        .expect("open versions file");
    writeln!(
        versions,
        "end {}",
        humantime::format_rfc3339_millis(SystemTime::now())
    )
    .expect("write end time");

    if !complete {
        eprintln!("a stream ended before the deadline; the capture is incomplete");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

/// Stream updates from one server into `tx` until the stream ends or the receiver is gone.
async fn stream_updates(
    node_id: usize,
    endpoint: String,
    x_token: Option<String>,
    versions_path: PathBuf,
    tx: mpsc::UnboundedSender<Row>,
) {
    let result = async {
        // Keepalive surfaces a silently dropped connection as a stream error.
        let mut client = GeyserGrpcClient::build_from_shared(endpoint.clone())?
            .x_token(x_token)?
            .connect_timeout(Duration::from_secs(10))
            .http2_keep_alive_interval(Duration::from_secs(10))
            .keep_alive_timeout(Duration::from_secs(5))
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .max_decoding_message_size(usize::MAX)
            .connect()
            .await?;

        let version = client.get_version().await?.version;
        eprintln!("node_id {node_id}: version {version}");

        // One short append per node, so the lines of concurrent nodes do not interleave.
        let mut versions = OpenOptions::new().append(true).open(&versions_path)?;
        writeln!(versions, "node_id {node_id} {endpoint} {version}")?;

        let request = SubscribeRequest {
            // 3400 per minute
            slots: HashMap::from([(
                "slots".to_owned(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(false),
                    interslot_updates: Some(true),
                },
            )]),
            // 3900 per minute
            transactions: HashMap::from([(
                "jup6".to_owned(),
                SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    account_include: vec![JUP6_PROGRAM.to_owned()],
                    ..Default::default()
                },
            )]),
            // 98000 per minute
            accounts: HashMap::from([(
                "token2022_accounts".to_owned(),
                SubscribeRequestFilterAccounts {
                    owner: vec![TOKEN2022_PROGRAM.to_string()],
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };

        // Hold the sink: older servers end the subscription when the request stream closes.
        let (_sink, mut stream) = client.subscribe_with_request(Some(request)).await?;

        while let Some(update) = stream.next().await {
            let update = update?;
            let created_at_us = update
                .created_at
                .map(|ts| ts.seconds * 1_000_000 + i64::from(ts.nanos) / 1_000);

            let row = match update.update_oneof {
                Some(UpdateOneof::Slot(slot)) => Row {
                    node_id,
                    created_at_us,
                    slot: slot.slot,
                    bank_id: slot.bank_id,
                    kind: RowKind::Slot {
                        status: slot.status,
                    },
                },
                Some(UpdateOneof::Transaction(update)) => {
                    let Some(info) = update.transaction else {
                        continue;
                    };

                    Row {
                        node_id,
                        created_at_us,
                        slot: update.slot,
                        bank_id: Some(update.bank_id),
                        kind: RowKind::Tx {
                            signature: info.signature,
                            index: info.index,
                        },
                    }
                },
                Some(UpdateOneof::Account(update)) => {
                    let Some(info) = update.account else { continue };

                    Row {
                        node_id,
                        created_at_us,
                        slot: update.slot,
                        bank_id: update.bank_id,
                        kind: RowKind::Account {
                            pubkey: info.pubkey,
                            write_version: info.write_version,
                        },
                    }
                },
                _ => continue,
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
/// SLOT,380012345,7,SLOT_CREATED_BANK,1762862400120001,0,,
/// SLOT,380012345,NO_BANK,SLOT_FIRST_SHRED_RECEIVED,1762862400124001,1,,
/// TX,380012345,7,,1762862400130512,0,5h6xBEauJ3PK6SWCZ1PGjBvj8vDdWG3KpwATGy1ARAXF...,412
/// ACCOUNT,380012345,7,,1762862400130977,0,cbbtcf3aa214zXHbiAZQwf4122FBYbraNdFqgw4iMij,1093
/// ```
fn write_row(out: &mut impl Write, row: &Row) -> std::io::Result<()> {
    let (kind, status, key, index) = match &row.kind {
        RowKind::Slot { status } => {
            let status = SlotStatus::try_from(*status)
                .map(|s| s.as_str_name().to_owned())
                .unwrap_or_else(|_| status.to_string());

            ("SLOT", status, String::new(), String::new())
        },
        RowKind::Tx { signature, index } => (
            "TX",
            String::new(),
            bs58::encode(signature).into_string(),
            index.to_string(),
        ),
        RowKind::Account {
            pubkey,
            write_version,
        } => (
            "ACC",
            String::new(),
            bs58::encode(pubkey).into_string(),
            write_version.to_string(),
        ),
    };

    // omitting "index"
    writeln!(
        out,
        "{kind},{},{},{status},{},{},{key}",
        row.slot,
        row.bank_id
            .map_or_else(|| "NO_BANK".to_owned(), |b| b.to_string()),
        row.created_at_us.map(|v| v.to_string()).unwrap_or_default(),
        row.node_id,
    )
}

use std::net::SocketAddr;
use std::path::PathBuf;
use clap::Parser;
use log::info;
use tokio::sync::mpsc;

use raft_nexus::consensus::MultiRaftNode;
use raft_nexus::rpc::server::RpcServer;

#[derive(Parser, Debug)]
#[command(author, version, about = "Raft-Nexus Distributed Multi-Raft Storage Node", long_about = None)]
struct Args {
    /// Unique identifier for this cluster node
    #[arg(short, long, default_value_t = 1)]
    node_id: u64,

    /// Bind socket address for TCP RPC communication
    #[arg(short, long, default_value = "127.0.0.1:9001")]
    addr: SocketAddr,

    /// Directory for Write-Ahead Log (WAL) and state snapshots
    #[arg(short, long, default_value = "./data")]
    data_dir: PathBuf,

    /// Comma-separated list of peer node IDs
    #[arg(short, long, value_delimiter = ',', default_values_t = vec![2, 3])]
    peers: Vec<u64>,

    /// Number of autonomous Multi-Raft groups to host
    #[arg(short, long, default_value_t = 1)]
    groups: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    info!(
        "Initializing Raft-Nexus node {} on {} with peers {:?}",
        args.node_id, args.addr, args.peers
    );

    let mut multi_node = MultiRaftNode::new(args.node_id, Some(args.data_dir));

    for g in 1..=args.groups {
        multi_node.add_group(g, args.peers.clone());
        info!("Registered consensus group {}", g);
    }

    let (inbound_tx, mut inbound_rx) = mpsc::unbounded_channel();
    let server = RpcServer::new(args.addr, inbound_tx);

    tokio::spawn(async move {
        if let Err(e) = server.run().await {
            log::error!("Server crashed: {}", e);
        }
    });

    let mut ticker = tokio::time::interval(std::time::Duration::from_millis(20));
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                multi_node.tick(std::time::Instant::now());
                let _outbound = multi_node.drain_outbound_messages();
            }
            Some(msg) = inbound_rx.recv() => {
                let _ = multi_node.handle_message(msg);
            }
        }
    }
}

use std::net::SocketAddr;
use std::path::PathBuf;
use log::info;
use tokio::sync::mpsc;

use raft_nexus::consensus::MultiRaftNode;
use raft_nexus::rpc::server::RpcServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    println!(r#"
  _____            __ _         _   _                      
 |  __ \          / _| |       | \ | |                     
 | |__) |__ _  __| |_| |_ _____|  \| | _____  ___   _ ___ 
 |  _  // _` |/ _` | | __|______| . ` |/ _ \ \/ / | | / __|
 | | \ \ (_| | (_| | | |_       | |\  |  __/>  <| |_| \__ \
 |_|  \_\__,_|\__,_|_|\__|      |_| \_|\___/_/\_\\__,_|___/
                                                           
    "#);
    info!("Starting Raft-Nexus Distributed Consensus Engine");

    let node_id: u64 = 1;
    let addr: SocketAddr = "127.0.0.1:9001".parse()?;
    let data_dir = PathBuf::from("./nexus-data");

    let mut multi_node = MultiRaftNode::new(node_id, Some(data_dir));
    // Initialize primary group 1
    multi_node.add_group(1, vec![2, 3]);

    let (inbound_tx, mut inbound_rx) = mpsc::unbounded_channel();
    let server = RpcServer::new(addr, inbound_tx);

    info!("Multi-Raft Node {} initialized for Group 1 on {}", node_id, addr);

    // Spawn network RPC listener
    tokio::spawn(async move {
        if let Err(e) = server.run().await {
            log::error!("RPC Server fatal error: {}", e);
        }
    });

    // Background cluster timer & message dispatch tick
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(20));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                multi_node.tick(std::time::Instant::now());
                let outbound = multi_node.drain_outbound_messages();
                if !outbound.is_empty() {
                    log::debug!("Outbound messages pending: {}", outbound.len());
                }
            }
            Some(msg) = inbound_rx.recv() => {
                let _ = multi_node.handle_message(msg);
            }
        }
    }
}

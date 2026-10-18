use std::net::SocketAddr;
use std::sync::Arc;
use log::{error, info};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::rpc::protocol::RaftMessage;
use crate::rpc::transport::TcpCodec;

/// Asynchronous network RPC server handling peer cluster and client traffic.
pub struct RpcServer {
    addr: SocketAddr,
    inbound_tx: mpsc::UnboundedSender<RaftMessage>,
}

impl RpcServer {
    pub fn new(addr: SocketAddr, inbound_tx: mpsc::UnboundedSender<RaftMessage>) -> Self {
        Self { addr, inbound_tx }
    }

    /// Runs the TCP listener event loop.
    pub async fn run(&self) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(self.addr).await?;
        info!("Raft-Nexus RPC Server listening on {}", self.addr);

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    let tx = self.inbound_tx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, tx).await {
                            error!("Connection error from {}: {}", peer_addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("TCP accept failed: {}", e);
                }
            }
        }
    }

    /// Handles an individual framed TCP connection.
    async fn handle_connection(
        mut stream: TcpStream,
        inbound_tx: mpsc::UnboundedSender<RaftMessage>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            match TcpCodec::read_message(&mut stream).await {
                Ok(msg) => {
                    if inbound_tx.send(msg).is_err() {
                        break; // Server shutting down
                    }
                }
                Err(crate::rpc::transport::TransportError::Io(ref e))
                    if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    break; // Clean disconnect
                }
                Err(e) => return Err(Box::new(e)),
            }
        }
        Ok(())
    }
}

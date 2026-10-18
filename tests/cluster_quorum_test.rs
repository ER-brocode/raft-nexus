use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

use raft_nexus::consensus::raft::RaftEngine;
use raft_nexus::consensus::role::RaftRole;
use raft_nexus::rpc::protocol::{ClientCommand, RaftMessage, RaftPayload};
use raft_nexus::rpc::transport::{ChannelTransport, Transport};

/// Helper simulating a cluster network step: delivering all messages queued in transport channels.
async fn deliver_messages(
    nodes: &mut [RaftEngine],
    inbound_rxs: &mut [mpsc::UnboundedReceiver<RaftMessage>],
    transport: &ChannelTransport,
) {
    // 1. Drain outbound messages from each node and send via transport
    for node in nodes.iter_mut() {
        for msg in node.outbound_messages.drain(..) {
            let _ = transport.send(msg).await;
        }
    }

    // 2. Deliver inbound messages to destination nodes
    for (idx, rx) in inbound_rxs.iter_mut().enumerate() {
        while let Ok(msg) = rx.try_recv() {
            let node = &mut nodes[idx];
            match msg.payload {
                RaftPayload::RequestVote(args) => {
                    let reply = node.handle_request_vote(args);
                    node.outbound_messages.push(RaftMessage {
                        group_id: msg.group_id,
                        from: node.node_id,
                        to: msg.from,
                        payload: RaftPayload::RequestVoteReply(reply),
                    });
                }
                RaftPayload::RequestVoteReply(reply) => {
                    node.handle_request_vote_reply(msg.from, reply);
                }
                RaftPayload::AppendEntries(args) => {
                    let reply = node.handle_append_entries(args);
                    node.outbound_messages.push(RaftMessage {
                        group_id: msg.group_id,
                        from: node.node_id,
                        to: msg.from,
                        payload: RaftPayload::AppendEntriesReply(reply),
                    });
                }
                RaftPayload::AppendEntriesReply(reply) => {
                    node.handle_append_entries_reply(msg.from, reply);
                }
                _ => {}
            }
        }
    }
}

#[tokio::test]
async fn test_three_node_cluster_election_and_quorum() {
    let transport = ChannelTransport::new();
    let mut inbound_rxs = Vec::new();

    let mut nodes = Vec::new();
    for id in 1..=3 {
        let (tx, rx) = mpsc::unbounded_channel();
        transport.register(id, tx);
        inbound_rxs.push(rx);

        let peers = (1..=3).filter(|&p| p != id).collect();
        let engine = RaftEngine::new(1, id, peers, None);
        nodes.push(engine);
    }

    // Node 1 starts election
    nodes[0].start_election();
    assert_eq!(nodes[0].state.soft_state.role, RaftRole::Candidate);
    assert_eq!(nodes[0].state.hard_state.current_term, 1);

    // Deliver vote requests and replies over two network steps
    deliver_messages(&mut nodes, &mut inbound_rxs, &transport).await;
    deliver_messages(&mut nodes, &mut inbound_rxs, &transport).await;

    // Node 1 should have received majority votes and become Leader
    assert_eq!(nodes[0].state.soft_state.role, RaftRole::Leader);
    assert_eq!(nodes[1].state.soft_state.role, RaftRole::Follower);
    assert_eq!(nodes[2].state.soft_state.role, RaftRole::Follower);

    // Leader proposes a client command
    let put_cmd = ClientCommand::Put {
        key: b"alpha".to_vec(),
        val: b"omega".to_vec(),
    };
    let index = nodes[0].propose(put_cmd, None).expect("Leader accepts proposal");
    assert_eq!(index, 2); // index 1 is initial No-Op, index 2 is Put

    // Replicate entry to followers
    deliver_messages(&mut nodes, &mut inbound_rxs, &transport).await;
    deliver_messages(&mut nodes, &mut inbound_rxs, &transport).await;
    deliver_messages(&mut nodes, &mut inbound_rxs, &transport).await;

    // Verify commit index has advanced across cluster
    assert_eq!(nodes[0].state.commit_index, 2);
    assert_eq!(nodes[1].state.commit_index, 2);
    assert_eq!(nodes[2].state.commit_index, 2);
}

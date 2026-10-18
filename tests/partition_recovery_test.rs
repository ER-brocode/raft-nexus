use tokio::sync::mpsc;
use raft_nexus::consensus::raft::RaftEngine;
use raft_nexus::consensus::role::RaftRole;
use raft_nexus::rpc::protocol::{ClientCommand, RaftMessage, RaftPayload};
use raft_nexus::rpc::transport::{ChannelTransport, Transport};

async fn step_network(
    nodes: &mut [RaftEngine],
    inbound_rxs: &mut [mpsc::UnboundedReceiver<RaftMessage>],
    transport: &ChannelTransport,
) {
    for node in nodes.iter_mut() {
        for msg in node.outbound_messages.drain(..) {
            let _ = transport.send(msg).await;
        }
    }

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
async fn test_partition_and_recovery() {
    let transport = ChannelTransport::new();
    let mut inbound_rxs = Vec::new();
    let mut nodes = Vec::new();

    for id in 1..=3 {
        let (tx, rx) = mpsc::unbounded_channel();
        transport.register(id, tx);
        inbound_rxs.push(rx);

        let peers = (1..=3).filter(|&p| p != id).collect();
        nodes.push(RaftEngine::new(1, id, peers, None));
    }

    // Node 1 becomes leader
    nodes[0].start_election();
    step_network(&mut nodes, &mut inbound_rxs, &transport).await;
    step_network(&mut nodes, &mut inbound_rxs, &transport).await;
    assert_eq!(nodes[0].state.soft_state.role, RaftRole::Leader);

    // Isolate Node 3 into a minority
    transport.isolate_pair(1, 3);
    transport.isolate_pair(2, 3);

    // Leader proposes while Node 3 is partitioned
    let cmd = ClientCommand::Put {
        key: b"k1".to_vec(),
        val: b"v1".to_vec(),
    };
    nodes[0].propose(cmd, None).unwrap();

    // Steps to replicate between Node 1 and Node 2
    step_network(&mut nodes, &mut inbound_rxs, &transport).await;
    step_network(&mut nodes, &mut inbound_rxs, &transport).await;

    // Node 1 & 2 committed because they have majority quorum (2 out of 3)
    assert_eq!(nodes[0].state.commit_index, 2);
    assert_eq!(nodes[1].state.commit_index, 2);
    // Node 3 remains uncommitted
    assert_ne!(nodes[2].state.commit_index, 2);

    // Heal partition
    transport.heal_all();

    // Replicate heartbeats
    nodes[0].send_heartbeats();
    step_network(&mut nodes, &mut inbound_rxs, &transport).await;
    step_network(&mut nodes, &mut inbound_rxs, &transport).await;

    // Node 3 catches up to commit index 2
    assert_eq!(nodes[2].state.commit_index, 2);
}

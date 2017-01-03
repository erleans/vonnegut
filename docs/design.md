Vonnegut Design Doc (1st Iteration)
-----------------------------------------

Vonnegut is an append only replicated log utilizing Kubernetes Stateful Sets for consistency and resource utilization.

## Log

Append only ordered sequence of records made up of multiple log segment files stored on disk.

## Log Segment

A log segment is a file with the name of its first contained log record id. The `active` log segment is the newest and the only one that has writes appended to it. When it or the corresponding index becomes too large a new active log segment is created. The index allows a reader to quickly find the start position of a record by id within a log segment.

## Chains

Chains consist of `N` vonnegut nodes, the first node in the chain is the `head` and the last is the `tail`. If `N=1` then these are the same and no replication occurs.

All writes are sent to the `head` of a chain, all reads occur on the `tail`. Dirty reads or historical reads (reads on data only in inactive log segments) can occur on any node in the chain.

## (not really) Virtual Nodes

Initial work will be on vonnegut nodes and virtual nodes having a 1 to 1 mapping. Meaning vonnegut nodes only take part in a single chain. Increasing the number of chains requires adding vonnegut nodes and overlapping chains across physical machines or virtual machines requires overlapping the separate vonnegut nodes. We'll be utilizing Kubernetes for handling the scheduling and resource utilization optimization for overlapping vonnegut nodes within a cluster of virtual machines.

## Cluster Membership

The vonnegut nodes form a cluster through finding nodes in DNS and [partisan](https://github.com/lasp-lang/partisan) for connecting and failure detection. When a failure is detected by partisan a call back is triggered on each node and the nodes wait to continue replication until the entire chain is healthy again.

Reads can continue as usual during failure. Unless, of course, it is the tail is unreachable by the clients, in which case the client requests will simply fail.

## Chain Membership

Chains are manually created and all nodes within a chain are added together to the cluster. No rebalancing is done within the cluster when a new chain is created. Instead, the weighted chain selection will return the new chain for new topics until the chains become balanced.

The order of a chain is a lexiographical sort of the node names guaranteeing each node sees the same chain structure. Nodes are named `<chain>-{0..N-1}` where `N` is the number of nodes in the chain.

## Adding Nodes to Existing Chains

New nodes are added to the end of the chain, Stateful Sets ordered node names ensures this. It is the responsibility of the current tail to promote the new node to the new tail after it has synced all topic log segments from the tail, at which point client requests for reads are redirected to the new tail and the reset of the members of the chain are notified that it is now `active`, making them capable of answering a client request for who is the current `tail`.

## Mapping Topics to Chains

New topics select a chain through randomly from the chains with the lowest weight. Weight can take into account numerous metrics of load but for starters will simply be the # topics on the chain.

## Replication

Chain replication is used for durability. Each write to the `head` is replicated to the next node in the chain, and so on until the `tail` is reached. Writes are acked from the `tail` to the client and on an interval the latest id written to disk is acked to the preceding on in a chain by each node except the `head`.

## Handling Failure

Each chain is a Kubernetes Stateful Set. A Stateful Set provides the ordering of the nodes and replacing a failed node with one of the same name and persistent storage. Thus in the case of partisan detecting a failure the chain will stop attempting to replicate and stop accepting new writies until healthy again.

Each node acks writes from its predecessor, `N-`, after receiving an ack from their successor, `N+`. Until receiving the `N+` ack all writes are kept in a history. In the event of a failure `N` sends the writes from the history to the new `N+`.

After an update is written (though possibly not flushed to disk depending on configuration) and sent to the next node in the chain it can continue to receive more updates without having recieved an `ack` from the next node in the chain. Nodes will periodically send acks with the latest record id they have written. Until an `ack` of an id larger or equal to a record it is kept in the history for possibly resending if a link has failed and it needs to send the writes to the new `N+`.

**To deal with potentially dropped messages by Erlang's messaging layer we need to rely on the fact record id is always increasing by 1 to force a failure. This failure notifies the predecessor to resend starting at the last in-order record received.**

The client is responsible for whether it wants to wait for an ack from the tail. It can keep sending writes before an ack arrives if it is ok with potentially losing writes.

**What happens if our process restarts and loses this in memory history of records not yet acked? We wouldn't want to have to consider the entire node dead when pontentially other topics are fine. Maybe an ets table under the top level supervisor is required for storing this information?**

**How do we handle a case of all nodes going down and having to resync with each other from what they have on disk?**

## Clients



## Permanent Subscriptions

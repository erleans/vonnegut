## Iteration 0 Implementation

### Cluster Manager

A single global process in the cluster, `vg_cluster_mgr`, is responsible for topic creation and mapping between topics and chains. This manager is to be an abstraction on top of multiple implementations for management, such as riak ensemble based consensus.

### ETS Tables

* `logs_segments_table`: This table is an in memory and index representation of the topic segments found on disk. This allows for a quick ets query to find the segment a specific offset is to be found. The segments index file is then searched to find the exact file position to read from.

* `high_watermarks_table`: Fetch responses must include the high watermark (highest message offset) for the topics included in the response. This value is tracked by a global ets table mapping topics to high watermarks. This value is updated after a messageset is written, so it does not get updated per message but per set written.

* `chains_table`

* `topic_map`: Requests bound for a specific topic must lookup the head or tail node in the chain that is responsible for that specific topic. This ets table is responsible for storing this mapping which is updated by querying the `cluster manager` directly or a `vg_client:metadata` or Kafka client metadata request to any node in the vonnegut cluster of chains.

### Client Refresh

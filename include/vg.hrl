-define(CLIENT_ID, "vg_client").
-define(MAX_REQUEST_ID, 2147483647).

-define(MAGIC, 1).
-define(API_VERSION, 1).

-define(PRODUCE_REQUEST, 0).
-define(FETCH_REQUEST, 1).
-define(METADATA_REQUEST, 3).

-define(COMPRESS_NONE, 0).
-define(COMPRESS_GZIP, 1).
-define(COMPRESS_SNAPPY, 2).
-define(COMPRESS_LZ4, 3).

-define(COMPRESSION_MASK, 7).
-define(COMPRESSION(Attr), ?COMPRESSION_MASK band Attr).

%% non-kafka extension
-define(TOPICS_REQUEST, 1000).

-define(NONE_ERROR, 0).
-define(FETCH_DISALLOWED_ERROR, 129).
-define(PRODUCE_DISALLOWED_ERROR, 131).

-define(SEGMENTS_TABLE, logs_segments_table).
-define(WATERMARK_TABLE, high_watermarks_table).
-define(CHAINS_TABLE, chains_table).

-define(topic_map, topic_map).

-record(chain, {
          name  :: binary() | atom(),
          nodes :: [atom()] | undefined,
          head  :: {inet:ip_address() | inet:hostname(), inet:port_number()},
          tail  :: {inet:ip_address() | inet:hostname(), inet:port_number()}
         }).
-type chain() :: #chain{}.

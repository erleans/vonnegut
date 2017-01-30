-define(CLIENT_ID, "vg_client").
-define(MAX_REQUEST_ID, 4294967296).

-define(MAGIC, 0).
-define(ATTRIBUTES, 0).
-define(API_VERSION, 1).

-define(PRODUCE_REQUEST, 0).
-define(FETCH_REQUEST, 1).
-define(METADATA_REQUEST, 3).

%% non-kafka extension
-define(TOPICS_REQUEST, 1000).

-define(NONE_ERROR, 0).
-define(FETCH_DISALLOWED_ERROR, 129).
-define(PRODUCE_DISALLOWED_ERROR, 131).

-define(SEGMENTS_TABLE, logs_segments_table).

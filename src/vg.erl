-module(vg).

-export([create_topic/1,
         ensure_topic/1,
         write/3,
         fetch/1, fetch/2,
         fetch/5]).

-include("vg.hrl").

-type record() :: #{id => integer(),
                    crc => integer(),
                    record := binary() | {binary(), binary()}}.
-type record_set() :: [record()].
-type topic() :: binary().

-export_types([topic/0,
               record/0,
               record_set/0]).

-spec create_topic(Topic :: topic()) -> ok.
create_topic(Topic) ->
    {ok, _Chain} = vg_cluster_mgr:create_topic(Topic),
    ok.

-spec ensure_topic(Topic :: topic()) -> ok.
ensure_topic(Topic) ->
    {ok, _Chain} = vg_cluster_mgr:ensure_topic(Topic),
    ok.

-spec write(Topic, Partition, Record) -> ok | {error, any()} when
      Topic :: topic(),
      Partition :: non_neg_integer(),
      Record :: binary() | record_set().
write(Topic, Partition, Record) when is_binary(Record) ->
    vg_active_segment:write(Topic, Partition, [#{record => Record}]);
write(Topic, Partition, RecordSet) when is_list(RecordSet) ->
    vg_active_segment:write(Topic, Partition, RecordSet).

fetch(Topic) ->
    fetch(Topic, 0).

-spec fetch(Topic, Offset) -> {ok, RecordSet} when
      Topic :: topic(),
      Offset :: integer(),
      RecordSet :: #{high_water_mark := integer(),
                      partition := 0,
                      record_set := record_set()}.
fetch(Topic, Offset) ->
    Partition = 0,
    {SegmentId, Position} = vg_log_segments:find_segment_offset(Topic, Partition, Offset),
    File = vg_utils:log_file(Topic, Partition, SegmentId),
    Size = filelib:file_size(File),
    {ok, Fd} = file:open(File, [read, binary, raw]),
    try
        {ok, [Data]} = file:pread(Fd, [{Position, Size}]),
        {ok, #{high_water_mark => vg_topics:lookup_hwm(Topic, Partition),
               partition => 0,
               record_set => vg_protocol:decode_record_set(Data, [])}}
    after
        file:close(Fd)
    end.

%% fetch/5 is a special form that only returns sizes and positions for
%% later framing and sending

%% A fetch of offset -1 returns Limit number of the records up to the
%% high watermark
fetch(Topic, Partition, -1, MaxBytes, Limit) ->
    Offset = vg_topics:lookup_hwm(Topic, Partition),
    fetch(Topic, Partition, erlang:max(0, Offset - Limit + 1), MaxBytes, Limit);
fetch(Topic, Partition, Offset, MaxBytes, Limit) ->
    {SegmentId, Position} = vg_log_segments:find_segment_offset(Topic, Partition, Offset),
    Fetch =
        case Limit of
            -1 ->
                unlimited;
            _ ->
                {EndSegmentId, EndPosition} =
                    vg_log_segments:find_segment_offset(Topic, Partition, Offset + Limit),
                case SegmentId of
                    %% max on this segment, limit fetch
                    EndSegmentId ->
                        {limited, EndPosition - Position};
                    %% some higher segment, unlimited fetch
                    _ ->
                        unlimited
                end
        end,

    lager:info("at=fetch_request topic=~s partition=~p offset=~p segment_id=~p position=~p",
              [Topic, Partition, Offset, SegmentId, Position]),

    File = vg_utils:log_file(Topic, Partition, SegmentId),
    SendBytes =
        case Fetch of
            unlimited ->
                filelib:file_size(File) - Position;
            {limited, Limited} ->
                Limited
        end,
    Bytes =
        case MaxBytes of
            0 -> SendBytes;
            _ -> min(SendBytes, MaxBytes)
        end,
    ErrorCode = 0,
    HighWaterMark = vg_topics:lookup_hwm(Topic, Partition),
    Response = vg_protocol:encode_fetch_topic_response(Partition, ErrorCode, HighWaterMark, Bytes),

    lager:debug("sending hwm=~p bytes=~p", [HighWaterMark, Bytes]),
    {erlang:iolist_size(Response)+Bytes, Response, {File, Position, Bytes}}.


-module(vg).

%% client interface
-export([ensure_topic/1,
         write_record_batch/3,
         write/3, write/4,
         fetch/1, fetch/2, fetch/4,
         fetch/5]).

%% ops interface.
-export([
         create_topic/1,
         delete_topic/1,
         describe_topic/1,
         deactivate_topic/1,
         regenerate_topic_index/1,
         tail_topic/1, tail_topic/2,
         running_topics/0
        ]).

-include("vg.hrl").

-type topic() :: binary().

-type record() :: #{offset => integer(),
                    timestamp => integer(),
                    key => binary(),
                    value := binary(),
                    headers => [{unicode:characters_binary(), binary()}]}.

-type record_batch() :: #{crc := integer(),
                          producer_id => integer(),
                          producer_epoch => integer(),
                          sequence_number => integer(),
                          records := [record()]}.

-export_types([topic/0,
               record/0,
               record_batch/0]).

-spec create_topic(Topic :: topic()) -> ok.
create_topic(Topic) ->
    case validate_topic(Topic) of
        ok ->
            {ok, _Chain} = vg_cluster_mgr:create_topic(Topic),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

-spec ensure_topic(Topic :: topic()) -> ok.
ensure_topic(Topic) ->
    case validate_topic(Topic) of
        ok ->
            {ok, _Chain} = vg_cluster_mgr:ensure_topic(Topic),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

validate_topic(B) when is_binary(B) ->
    Disallowed =
        [
         <<0>>,
         <<"/">>, % path separators
         <<"\\">>,
         <<"*">>,
         <<".">>, <<"..">>,
         <<"[">>, <<"]">>,
         <<"(">>, <<")">>,
         <<"{">>, <<"}">>
        ],
    case binary:match(B, Disallowed) of
        nomatch ->
            ok;
        _ ->
            {error, invalid_characters}
    end;
validate_topic(_) ->
    {error, non_binary_topic}.

-spec write_record_batch(Topic, Partition, RecordBatch) -> {ok, integer()} | {error, any()} when
      Topic :: topic(),
      Partition :: non_neg_integer(),
      RecordBatch :: vg:record_batch().
write_record_batch(Topic, Partition, RecordBatch) ->
    vg_active_segment:write(Topic, Partition, RecordBatch).

-spec write(Topic, Partition, Records) -> ok | {error, any()} when
      Topic :: topic(),
      Partition :: non_neg_integer(),
      Records :: binary() | [binary()].
write(Topic, Partition, Records) ->
    RecordBatch = vg_protocol:encode_record_batch(Records),
    vg_active_segment:write(Topic, Partition, RecordBatch).

write(Topic, Partition, ExpectedId, RecordBatch) ->
    vg_active_segment:write(Topic, Partition, ExpectedId, RecordBatch).

fetch(Topic) ->
    fetch(Topic, 0).

-spec fetch(Topic, Offset) -> {ok, RecordBatches} when
      Topic :: topic(),
      Offset :: integer(),
      RecordBatches :: #{high_water_mark := integer(),
                         partition := 0,
                         record_batches := [vg:record_batch()]}.
fetch(Topic, Offset) ->
    fetch(Topic, 0, Offset, -1).

fetch(Topic, Partition, Offset, Count) ->
    {_, _, {File, Position, Bytes}} = fetch(Topic, Partition, Offset, 0, Count),
    {ok, Fd} = file:open(File, [read, binary, raw]),
    try
        {ok, [Data]} = file:pread(Fd, [{Position, Bytes}]),
        {ok, #{high_water_mark => vg_topics:lookup_hwm(Topic, Partition),
               partition => Partition,
               record_batches => vg_protocol:decode_record_batches(Data)}}
    after
        file:close(Fd)
    end.

%% fetch/5 is a special form that only returns sizes and positions for
%% later framing and sending

%% A fetch of offset -1 returns Limit number of the records up to the
%% high watermark
fetch(Topic, Partition, -1, MaxBytes, Limit) ->
    HWM = vg_topics:lookup_hwm(Topic, Partition),
    fetch(Topic, Partition, erlang:max(0, HWM - Limit + 1), MaxBytes, Limit);
fetch(Topic, Partition, Offset, MaxBytes, Limit) ->
    {SegmentId, {Position, _}} = vg_log_segments:find_segment_offset(Topic, Partition, Offset),
    File = vg_utils:log_file(Topic, Partition, SegmentId),
    SendBytes =
        case Limit of
            -1 ->
                filelib:file_size(File) - Position;
            _ ->
                LastOffset = Offset + Limit,
                case vg_log_segments:find_log_segment(Topic, Partition, LastOffset) of
                    %% lastoffset is on the same segment, so limit fetch to lastoffset position
                    SegmentId ->
                        {EndPosition, EndSize} =
                            vg_log_segments:find_record_offset(Topic, Partition, SegmentId, LastOffset),
                        case EndPosition of
                            Position ->
                                %% in the same RecordBatch
                                EndSize;
                            _ ->
                                (EndPosition + EndSize) - Position
                        end;
                    %% some higher segment, so send this whole segment
                    _ ->
                        filelib:file_size(File) - Position
                end
        end,

    lager:info("at=fetch_request topic=~s partition=~p offset=~p segment_id=~p position=~p",
               [Topic, Partition, Offset, SegmentId, Position]),

    Bytes =
        case MaxBytes of
            0 -> SendBytes;
            _ -> min(SendBytes, MaxBytes)
        end,
    ErrorCode = 0,
    HWM = vg_topics:lookup_hwm(Topic, Partition),
    Response = vg_protocol:encode_fetch_topic_response(Partition, ErrorCode, HWM, Bytes),
    lager:debug("sending hwm=~p bytes=~p", [HWM, Bytes]),
    {erlang:iolist_size(Response)+Bytes, Response, {File, Position, Bytes}}.

%% these are here mostly for ergonomics.  right now they just forward
%% the work to the cluster manager, but we might need to change that
%% later and this allows us to keep a easy to type interface that
%% doesn't have to change.
delete_topic(Topic) ->
    vg_cluster_mgr:delete_topic(Topic).

describe_topic(Topic) ->
    vg_cluster_mgr:describe_topic(Topic).

deactivate_topic(Topic) ->
    vg_cluster_mgr:deactivate_topic(Topic).

%% there's a debate here to be had about doing this all at once vs. a
%% per segment approach.  wrt to format changes (which should be
%% ultra-rare), this is the right thing, but wrt index corruption
%% (which should also be super rare?), we might want the fine control
%% of regenerating a particular segment's index alone.
regenerate_topic_index(Topic) ->
    vg_topic_mgr:regenerate_index(Topic, 0).

tail_topic(Topic) ->
    tail_topic(Topic, #{}).

-spec tail_topic(binary(), Opts) -> ok when
      Opts :: #{records => pos_integer(), % default 10 records
                time => pos_integer()}.   % default 30 seconds
tail_topic(Topic, Opts) ->
    Printer = erlang:spawn_opt(fun() -> tail_printer(Topic, Opts) end,
                               [{max_heap_size, 1024 * 1024}]),
    vg_active_segment:tail(Topic, 0, Printer).

%% this is shaping up to be quite expensive and could block lazy
%% starts of deactivated topics.  use in production with caution.
running_topics() ->
    vg_cluster_mgr:running_topics().

tail_printer(Topic, Opts) ->
    Records = maps:get(records, Opts, 10),
    Time = maps:get(time, Opts, timer:seconds(30)),
    EndTime = erlang:monotonic_time(milli_seconds) + Time,
    F = fun Loop(0, _End) ->
                io:format("printed ~p records, terminating~n", [Records]);
            Loop(R, End) ->
                Left = End - erlang:monotonic_time(milli_seconds),
                case Left > 0 of
                    true ->
                        receive
                            {'$print', Term} ->
                                io:format("~p: ~p~n", [Topic, Term]),
                                Loop(R - 1, End)
                        after Left ->
                                io:format("tail session timed out~n")
                        end;
                    false ->
                        io:format("tail session timed out~n")
                end
        end,
    F(Records, EndTime).

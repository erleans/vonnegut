%%
-module(vg_active_segment).

-behaviour(gen_statem).

-export([start_link/3,
         write/3,
         write/4,
         halt/2,
         tail/3,
         where/2,
         stop_indexing/2,
         resume_indexing/2]).

-export([init/1,
         callback_mode/0,
         active/3,
         halted/3,
         handle_event/3,
         terminate/3]).

-include("vg.hrl").

-record(config, {log_dir              :: file:filename(),
                 segment_bytes        :: integer(),
                 index_max_bytes      :: integer(),
                 index_interval_bytes :: integer()}).

-record(data, {topic_dir       :: file:filename(),
               next_id         :: integer(),
               next_brick      :: atom(),
               byte_count      :: integer(),
               pos             :: integer(),
               index_pos       :: integer(),
               log_fd          :: file:fd(),
               segment_id      :: integer(),
               index_fd        :: file:fd() | undefined,
               topic           :: binary(),
               partition       :: integer(),
               config          :: #config{},
               halted = false  :: boolean(),
               index = true    :: boolean(),
               tailer          :: pid() | undefined,
               terminate_after :: integer(),
               timer_ref       :: reference()
              }).

%% need this until an Erlang release with `hibernate_after` spec added to gen option type
-dialyzer({nowarn_function, start_link/3}).

-define(ACTIVE_SEG(Topic, Partition), {via, gproc, {n, l, {active, Topic, Partition}}}).

start_link(Topic, Partition, NextBrick) ->
    HibernateAfter = application:get_env(vonnegut, hibernate_after, timer:minutes(1)),
    case gen_statem:start_link(?ACTIVE_SEG(Topic, Partition), ?MODULE, [Topic, Partition, NextBrick],
                               [{hibernate_after, HibernateAfter}]) of % hibernate after 5 minutes with no messages
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

-spec write(Topic, Partition, RecordBatch) -> {ok, Offset} | {error, any()} when
      Topic :: binary(),
      Partition :: integer(),
      RecordBatch :: vg:record_batch() | [vg:record_batch()],
      Offset :: integer().
write(Topic, Partition, RecordBatch) ->
    write(Topic, Partition, head, RecordBatch).

write(Topic, Partition, ExpectedId, [RecordBatch]) ->
    write_(Topic, Partition, ExpectedId, RecordBatch);
write(Topic, Partition, ExpectedId, RecordBatch) ->
    write_(Topic, Partition, ExpectedId, RecordBatch).

write_(Topic, Partition, ExpectedId, RecordBatch) ->
    try
        case gen_statem:call(?ACTIVE_SEG(Topic, Partition), {write, ExpectedId, RecordBatch}) of
            retry ->
                write_(Topic, Partition, ExpectedId, RecordBatch);
            R -> R
        end
    catch _:{noproc, _} ->
            create_retry(Topic, Partition, ExpectedId, RecordBatch);
          error:badarg ->  %% is this too broad?  how to restrict?
            create_retry(Topic, Partition, ExpectedId, RecordBatch);
          exit:{timeout, _} ->
            {error, timeout}
    end.

create_retry(Topic, Partition, ExpectedId, RecordBatch)->
    lager:warning("write to nonexistent topic '~s', creating", [Topic]),
    {ok, _} = vg_cluster_mgr:ensure_topic(Topic),
    write_(Topic, Partition, ExpectedId, RecordBatch).

halt(Topic, Partition) ->
    gen_statem:call(?ACTIVE_SEG(Topic, Partition), halt).

tail(Topic, Partition, Printer) ->
    gen_statem:call(?ACTIVE_SEG(Topic, Partition), {tail, Printer}).

where(Topic, Partition) ->
    {_, _, Where} = ?ACTIVE_SEG(Topic, Partition),
    gproc:where(Where).

stop_indexing(Topic, Partition) ->
    gen_statem:call(?ACTIVE_SEG(Topic, Partition), stop_indexing).

resume_indexing(Topic, Partition) ->
    gen_statem:call(?ACTIVE_SEG(Topic, Partition), resume_indexing).

%%%%%%%%%%%%

init([Topic, Partition, NextNode]) ->
    lager:info("at=init topic=~p next_server=~p", [Topic, NextNode]),
    Config = setup_config(),
    Partition = 0,
    LogDir = Config#config.log_dir,
    TerminateAfter = application:get_env(vonnegut, terminate_after, timer:minutes(5)),
    TopicDir = filename:join(LogDir, [binary_to_list(Topic), "-", integer_to_list(Partition)]),
    filelib:ensure_dir(filename:join(TopicDir, "ensure")),

    vg_log_segments:load_all(Topic, Partition),

    {Id, LatestIndex, LatestLog} = vg_log_segments:find_latest_id(TopicDir, Topic, Partition),
    LastLogId = filename:basename(LatestLog, ".log"),
    {ok, LogFD} = vg_utils:open_append(LatestLog),
    {ok, IndexFD} = vg_utils:open_append(LatestIndex),

    {ok, Position} = file:position(LogFD, eof),
    {ok, IndexPosition} = file:position(IndexFD, eof),

    vg_topics:insert_hwm(Topic, Partition, Id),

    {ok, active, #data{next_id = Id + 1,
                       next_brick = NextNode,
                       topic_dir = TopicDir,
                       byte_count = 0,
                       pos = Position,
                       index_pos = IndexPosition,
                       log_fd = LogFD,
                       segment_id = list_to_integer(LastLogId),
                       index_fd = IndexFD,
                       topic = Topic,
                       partition = Partition,
                       config = Config,
                       terminate_after = TerminateAfter,
                       timer_ref = erlang:start_timer(TerminateAfter, self(), terminate)
                      }}.

callback_mode() ->
    state_functions.

%% keep any new writes from coming in while we delete the topic
halted({call, From}, _, _) ->
    {keep_state_and_data, [{reply, From, halted}]}.

active({call, From}, halt, Data) ->
    {next_state, halted, Data, [{reply, From, ok}]};
active({call, From}, {tail, Printer}, Data) ->
    monitor(process, Printer),
    {keep_state, Data#data{tailer = Printer}, [{reply, From, ok}]};
active({call, From}, stop_indexing, Data=#data{index_fd=undefined}) ->
    {keep_state, Data#data{index = false}, [{reply, From, ok}]};
active({call, From}, stop_indexing, Data=#data{index_fd=FD}) ->
    %% no need to sync here, we're about to unlink
    file:close(FD),
    {keep_state, Data#data{index = false, index_fd = undefined}, [{reply, From, ok}]};
active({call, From}, resume_indexing, Data) ->
    {keep_state, Data#data{index = true}, [{reply, From, ok}]};
active({call, From}, {write, ExpectedID0, Record=#{last_offset_delta := LastOffsetDelta,
                                                   record_batch := RecordBatch}}, Data=#data{next_id=ID,
                                                                                             tailer=Tailer,
                                                                                             topic=Topic,
                                                                                             next_brick=NextBrick,
                                                                                             terminate_after=TerminateAfter,
                                                                                             timer_ref=TRef}) ->
    erlang:cancel_timer(TRef),
    TRef1 = erlang:start_timer(TerminateAfter, self(), terminate),
    Data1 = Data#data{timer_ref=TRef1},

    %% TODO: add pipelining of requests
    try
        ExpectedID =
            case ExpectedID0 of
                head ->
                    ID + LastOffsetDelta + 1;
                Supplied when is_integer(Supplied) ->
                    case (ID + LastOffsetDelta + 1) == Supplied of
                        true ->
                            ExpectedID0;
                        %% should we check > vs < here?  one is repair
                        %% the other is bad corruption
                        _ ->
                            %% inferred current id of the writing segment
                            WriterID = ExpectedID0 - LastOffsetDelta,
                            %% this should probably be limited, if
                            %% we're going back too far, we need to be
                            %% in some sort of catch-up mode
                            lager:debug("starting write repair, ~p", [WriterID]),
                            WriteRepairSet = write_repair(WriterID, Data1),
                            throw({write_repair, WriteRepairSet, Data1})
                    end
            end,

        Result =
            case NextBrick of
                Role when Role == solo; Role == tail -> proceed;
                _ ->
                    (fun Loop(_, Remaining) when Remaining =< 0 ->
                             {error, timeout};
                         Loop(Start, Remaining) ->
                             case vg_client:replicate(next_brick, Topic, ExpectedID, RecordBatch, Remaining) of
                                 retry ->
                                     Now = erlang:monotonic_time(milli_seconds),
                                     Elapsed = Now - Start,
                                     Loop(Now, Remaining - Elapsed);
                                 Result ->
                                     Result
                             end
                     end)(erlang:monotonic_time(milli_seconds), timeout() * 5)
            end,

        case Result of
            Go when Go =:= proceed orelse
                    element(1, Go) =:= ok ->
                Data2 = write_record_batch(Record, Data1),
                case Tailer of
                    undefined ->
                        ok;
                    Pid ->
                        Pid ! {'$print', {Data2#data.next_id - 1, Record}}
                end,
                {keep_state, Data2, [{reply, From, {ok, Data2#data.next_id - 1}}]};
            {write_repair, RepairSet} ->
                prometheus_counter:inc(write_repairs),
                %% add in the following when pipelining is added, if it makes sense
                %% prometheus_gauge:inc(pending_write_repairs, length(RepairSet)),
                Data2 = write_record_batch(RepairSet, Data1),
                case ExpectedID0 of
                    head ->
                        {keep_state, Data2, [{reply, From, retry}]};
                    _ ->
                        {keep_state, Data2, [{reply, From, {write_repair, RepairSet}}]}
                end;
            {error, Reason} ->
                {keep_state, Data1, [{reply, From, {error, Reason}}]}
        end
    catch throw:{write_repair, RS, D} ->
            {keep_state, D, [{reply, From, {write_repair, RS}}]};
          throw:{E, D} ->
            {keep_state, D, [{reply, From, {error, E}}]}
    end;
active(Type, Event, Data) ->
    handle_event(Type, Event, Data).


handle_event(info, {timeout, _TRef, terminate}, _Data) ->
    {stop, normal};
handle_event(info, {'DOWN', _MonitorRef, _Type, _Object, _Info}, Data) ->
    {keep_state, Data#data{tailer = undefined}}.

terminate(_, _Reason, _Data=#data{log_fd=LogFile,
                                  index_fd=IndexFile}) ->
    file:close(LogFile),
    file:close(IndexFile),
    ok.

%

write_record_batch(Batches, Data) when is_list(Batches) ->
    lists:foldl(fun(Batch, DataAcc) ->
                        write_record_batch(Batch, DataAcc)
                end, Data, Batches);
write_record_batch(#{last_offset_delta := LastOffsetDelta,
                     size := Size0,
                     record_batch := Bytes}, Data=#data{topic=Topic,
                                                        partition=Partition,
                                                        next_id=Id,
                                                        byte_count=ByteCount}) ->
    Size = Size0 + ?OFFSET_AND_LENGTH_BYTES,
    NextId = Id + LastOffsetDelta + 1,
    Data1 = #data{pos=Position1,
                  log_fd=LogFile} = maybe_roll(Size, Data),

    %% write to log
    ok = file:write(LogFile, [<<Id:64/signed-integer, Size0:32/signed-integer>>, Bytes]),
    Data2 = Data1#data{byte_count=ByteCount+Size},

    %% maybe write index entry
    Data3 = update_index(Data2),

    %% update highwatermark in ets table
    vg_topics:update_hwm(Topic, Partition, NextId-1),

    Data3#data{next_id=NextId,
               pos=Position1+Size}.

%% Create new log segment and index file if current segment is too large
%% or if the index file is over its max and would be written to again.
maybe_roll(Size, Data=#data{next_id=Id,
                            topic_dir=TopicDir,
                            log_fd=LogFile,
                            index_fd=IndexFile,
                            pos=Position,
                            byte_count=ByteCount,
                            index_pos=IndexPosition,
                            index = Indexing,
                            topic=Topic,
                            partition=Partition,
                            config=#config{segment_bytes=SegmentBytes,
                                           index_max_bytes=IndexMaxBytes,
                                           index_interval_bytes=IndexIntervalBytes}})
  when Position+Size > SegmentBytes
       orelse (ByteCount+Size >= IndexIntervalBytes
               andalso IndexPosition+?INDEX_ENTRY_SIZE > IndexMaxBytes) ->
    lager:debug("seg size ~p max size ~p", [Position+Size, SegmentBytes]),
    lager:debug("index interval size ~p max size ~p", [ByteCount+Size, IndexIntervalBytes]),
    lager:debug("index pos ~p max size ~p", [IndexPosition+?INDEX_ENTRY_SIZE, IndexMaxBytes]),
    ok = file:sync(LogFile),
    ok = file:close(LogFile),

    case Indexing of
        true ->
            ok = file:sync(IndexFile),
            ok = file:close(IndexFile);
        _ ->
            ok
    end,

    {NewIndexFile, NewLogFile} = vg_log_segments:new_index_log_files(TopicDir, Id),
    vg_log_segments:insert(Topic, Partition, Id),

    Data#data{log_fd=NewLogFile,
              index_fd=NewIndexFile,
              %% we assume here that new indexes are good, and
              %% re-enable writing, expecting the old indexes to
              %% catch up eventually.  This might be racy
              index = true,
              segment_id = Id,
              byte_count=0,
              pos=0,
              index_pos=0};
maybe_roll(_, Data) ->
    Data.

%% skip writing indexes if they're disabled.
update_index(Data=#data{index = false}) ->
    Data;
%% Add to index if the number of bytes written to the log since the last index record was written
update_index(Data=#data{next_id=Id,
                        pos=Position,
                        index_fd=IndexFile,
                        byte_count=ByteCount,
                        index_pos=IndexPosition,
                        segment_id=BaseOffset,
                        config=#config{index_interval_bytes=IndexIntervalBytes}})
  when ByteCount >= IndexIntervalBytes ->
    IndexEntry = <<(Id - BaseOffset):?INDEX_OFFSET_BITS/unsigned, Position:?INDEX_OFFSET_BITS/unsigned>>,
    ok = file:write(IndexFile, IndexEntry),
    Data#data{index_pos=IndexPosition+?INDEX_ENTRY_SIZE,
                byte_count=0};
update_index(Data) ->
    Data.

write_repair(Start, #data{next_id = ID, topic = Topic, partition = Partition}) ->
    %% two situations: replaying single-segment writes, and writes
    %% that span multiple segments
    {StartSegmentID, {StartPosition, _}} = vg_log_segments:find_segment_offset(Topic, Partition, Start),
    {EndSegmentID, {EndPosition, EndSize}} = vg_log_segments:find_segment_offset(Topic, Partition, ID),
    File = vg_utils:log_file(Topic, Partition, StartSegmentID),
    lager:debug("at=write_repair file=~p start=~p end=~p", [File, StartPosition, EndPosition]),
    case StartSegmentID == EndSegmentID of
        true ->
            {ok, FD} = file:open(File, [read, binary, raw]),
            try
                {ok, Data} = file:pread(FD, StartPosition, (EndPosition + EndSize) - StartPosition),
                [{StartSegmentID, Data}]
            after
                file:close(FD)
            end;
        _ ->
            error(not_implemented)
    end.

setup_config() ->
    {ok, [LogDir]} = application:get_env(vonnegut, log_dirs),
    {ok, SegmentBytes} = application:get_env(vonnegut, segment_bytes),
    {ok, IndexMaxBytes} = application:get_env(vonnegut, index_max_bytes),
    {ok, IndexIntervalBytes} = application:get_env(vonnegut, index_interval_bytes),
    #config{log_dir=LogDir,
            segment_bytes=SegmentBytes,
            index_max_bytes=IndexMaxBytes,
            index_interval_bytes=IndexIntervalBytes}.

timeout() ->
    application:get_env(vonnegut, ack_timeout, 1000).

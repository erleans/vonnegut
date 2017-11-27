%%
-module(vg_active_segment).

-behaviour(gen_server).

-export([start_link/3,
         write/3, write/4,
         ack/3]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(config, {log_dir              :: file:filename(),
                 segment_bytes        :: integer(),
                 index_max_bytes      :: integer(),
                 index_interval_bytes :: integer()}).

-record(state, {topic_dir   :: file:filename(),
                id          :: integer(),
                next_brick  :: {node(), atom()},
                byte_count  :: integer(),
                pos         :: integer(),
                index_pos   :: integer(),
                log_fd      :: file:fd(),
                segment_id  :: integer(),
                index_fd    :: file:fd(),
                topic       :: binary(),
                partition   :: integer(),
                history_tab :: atom(),
                config      :: #config{}
               }).

-define(NEW_SERVER(Topic, Partition),
        binary_to_atom(<<Topic/binary, $-,
                         (integer_to_binary(Partition))/binary>>, latin1)).
-define(SERVER(Topic, Partition),
        binary_to_existing_atom(<<Topic/binary, $-,
                                  (integer_to_binary(Partition))/binary>>, latin1)).

start_link(Topic, Partition, NextBrick) ->
    %% worth the trouble of making sure we have no acks table on tails?
    Tab = vg_pending_writes:ensure_tab(Topic, Partition),
    gen_server:start_link({local, ?NEW_SERVER(Topic, Partition)}, ?MODULE, [Topic, Partition, NextBrick, Tab],
                          [{hibernate_after, timer:minutes(5)}]). %% hibernate after 5 minutes with no messages

-spec write(Topic, Partition, RecordSet) -> {ok, Offset} | {error, any()} when
      Topic :: binary(),
      Partition :: integer(),
      RecordSet :: vg:record_set(),
      Offset :: integer().
write(Topic, Partition, RecordSet) ->
    %% there clearly needs to be a lot more logic here.  it's also not
    %% clear that this is the right place for this
    try
        gen_server:call(?SERVER(Topic, Partition), {write, RecordSet})
    catch _:{noproc, _} ->
            lager:warning("write to nonexistent topic '~s', creating", [Topic]),
            {ok, _} = vg_cluster_mgr:ensure_topic(Topic),
            write(Topic, Partition, RecordSet)
    end.

%% TODO: clean up the duplication from all this
write(Sender, Topic, Partition, RecordSet) ->
    %% there clearly needs to be a lot more logic here.  it's also not
    %% clear that this is the right place for this
    try
        gen_server:call(?SERVER(Topic, Partition), {write, Sender, RecordSet})
    catch _:{noproc, _} ->
            lager:warning("sender write to nonexistent topic '~s', creating", [Topic]),
            {ok, _} = vg_cluster_mgr:ensure_topic(Topic),
            write(Sender, Topic, Partition, RecordSet)
    end.

ack(Topic, Partition, LatestId) ->
    vg_pending_writes:ack(Topic, Partition, LatestId).

init([Topic, Partition, NextNode, Tab]) ->
    lager:info("at=init topic=~p next_server=~p", [Topic, NextNode]),
    Config = setup_config(),
    Partition = 0,
    LogDir = Config#config.log_dir,
    TopicDir = filename:join(LogDir, [binary_to_list(Topic), "-", integer_to_list(Partition)]),
    filelib:ensure_dir(filename:join(TopicDir, "ensure")),

    {Id, LatestIndex, LatestLog} = vg_log_segments:find_latest_id(TopicDir, Topic, Partition),
    LastLogId = filename:basename(LatestLog, ".log"),
    {ok, LogFD} = vg_utils:open_append(LatestLog),
    {ok, IndexFD} = vg_utils:open_append(LatestIndex),

    {ok, Position} = file:position(LogFD, eof),
    {ok, IndexPosition} = file:position(IndexFD, eof),

    vg_topics:update_hwm(Topic, Partition, Id - 1),

    {ok, #state{id=Id,
                next_brick={?SERVER(Topic, Partition), NextNode},
                topic_dir=TopicDir,
                byte_count=0,
                pos=Position,
                index_pos=IndexPosition,
                log_fd=LogFD,
                segment_id=list_to_integer(LastLogId),
                index_fd=IndexFD,
                topic=Topic,
                partition=Partition,
                config=Config,
                history_tab=Tab
               }}.

handle_call({write, RecordSet}, _From, State=#state{id=Id,
                                                    topic=Topic,
                                                    partition=Partition,
                                                    next_brick=NextBrick}) ->
    State1 = write_record_set(RecordSet, State),
    case NextBrick of
        {_, solo} -> ok;
        {_, tail} -> ok;
        {_, last} -> ok;
        _ ->
            gen_server:cast(NextBrick, {write, self(), RecordSet})
    end,
    LatestId = State1#state.id,
    %% Should we do this in ack instead if it isn't solo or tail?
    vg_topics:update_hwm(Topic, Partition, LatestId-1),
    {reply, {ok, Id}, State1};
handle_call(_Msg, _From, State) ->
    lager:info("bad call ~p ~p", [_Msg, _From]),
    {noreply, State}.

handle_cast({write, Sender, RecordSet}, State=#state{topic=Topic,
                                                     partition=Partition,
                                                     next_brick=NextBrick}) ->
    State1 = write_record_set(RecordSet, State),
    case NextBrick of
        {_, solo} -> ok;
        {_, tail} -> ok;
        {_, last} -> ok;
        _ ->
            gen_server:cast(NextBrick, {write, self(), RecordSet})
    end,

    LatestId = State1#state.id,
    %% Should we do this in ack instead if it isn't solo or tail?
    vg_topics:update_hwm(Topic, Partition, LatestId-1),
    %% TODO: batch acks
    Sender ! {ack, LatestId},

    {noreply, State1};
handle_cast(_Msg, State) ->
    lager:info("bad cast ~p", [_Msg]),
    {noreply, State}.

handle_info({ack, ID}, State) ->
    ack(State#state.topic, State#state.partition, ID),
    {noreply, State};
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

%%

write_record_set(RecordSet, State=#state{history_tab=Tab, next_brick=Next}) ->
    lists:foldl(fun(Record, StateAcc=#state{id=Id,
                                            byte_count=ByteCount}) ->
                    {NextId, Size, Bytes} = vg_protocol:encode_log(Id, Record),
                    StateAcc1 = #state{pos=Position1} = maybe_roll(Size, StateAcc),
                    update_log(Bytes, StateAcc1),
                    StateAcc2 = StateAcc1#state{byte_count=ByteCount+Size},
                    StateAcc3 = update_index(StateAcc2),

                    case Next of
                        %%solo -> ok;
                        {_, last} -> ok;
                        {_, tail} -> ok;
                        _ ->
                            vg_pending_writes:add(Tab, Id, Bytes)
                    end,

                    StateAcc3#state{id=NextId,
                                    pos=Position1+Size}
                end, State, RecordSet).

%% Create new log segment and index file if current segment is too large
%% or if the index file is over its max and would be written to again.
maybe_roll(Size, State=#state{id=Id,
                              topic_dir=TopicDir,
                              log_fd=LogFile,
                              index_fd=IndexFile,
                              pos=Position,
                              byte_count=ByteCount,
                              index_pos=IndexPosition,
                              topic=Topic,
                              partition=Partition,
                              config=#config{segment_bytes=SegmentBytes,
                                             index_max_bytes=IndexMaxBytes,
                                             index_interval_bytes=IndexIntervalBytes}})
  when Position+Size > SegmentBytes
     orelse (ByteCount+Size >= IndexIntervalBytes
            andalso IndexPosition+6 > IndexMaxBytes) ->
    lager:debug("seg size ~p max size ~p", [Position+Size, SegmentBytes]),
    lager:debug("index interval size ~p max size ~p", [ByteCount+Size, IndexIntervalBytes]),
    lager:debug("index pos ~p max size ~p", [IndexPosition+6, IndexMaxBytes]),
    ok = file:sync(LogFile),
    ok = file:sync(IndexFile),
    ok = file:close(LogFile),
    ok = file:close(IndexFile),

    {NewIndexFile, NewLogFile} = vg_log_segments:new_index_log_files(TopicDir, Id),
    vg_log_segments:insert(Topic, Partition, Id),

    State#state{log_fd=NewLogFile,
                index_fd=NewIndexFile,
                byte_count=0,
                pos=0,
                index_pos=0};
maybe_roll(_, State) ->
    State.

%% Append encoded log to segment
update_log(Record, #state{log_fd=LogFile}) ->
    ok = file:write(LogFile, Record).

%% Add to index if the number of bytes written to the log since the last index record was written
update_index(State=#state{id=Id,
                          pos=Position,
                          index_fd=IndexFile,
                          byte_count=ByteCount,
                          index_pos=IndexPosition,
                          segment_id=BaseOffset,
                          config=#config{index_interval_bytes=IndexIntervalBytes}})
  when ByteCount >= IndexIntervalBytes ->
    IndexEntry = <<(Id - BaseOffset):24/signed, Position:24/signed>>,
    ok = file:write(IndexFile, IndexEntry),
    State#state{index_pos=IndexPosition+6,
                byte_count=0};
update_index(State) ->
    State.

setup_config() ->
    {ok, [LogDir]} = application:get_env(vonnegut, log_dirs),
    {ok, SegmentBytes} = application:get_env(vonnegut, segment_bytes),
    {ok, IndexMaxBytes} = application:get_env(vonnegut, index_max_bytes),
    {ok, IndexIntervalBytes} = application:get_env(vonnegut, index_interval_bytes),
    #config{log_dir=LogDir,
            segment_bytes=SegmentBytes,
            index_max_bytes=IndexMaxBytes,
            index_interval_bytes=IndexIntervalBytes}.

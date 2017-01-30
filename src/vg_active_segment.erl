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
                next_brick  :: node(),
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

-define(SERVER(Topic, Partition), {via, gproc, {n,l,{Topic,Partition}}}).

start_link(Topic, Partition, NextBrick) ->
    %% worth the trouble of making sure we have no acks table on tails?
    Tab = vg_pending_writes:ensure_tab(Topic, Partition),
    gen_server:start_link(?SERVER(Topic, Partition), ?MODULE, [Topic, Partition, NextBrick, Tab], []).

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
            {ok, _} = vonnegut_sup:create_topic(Topic),
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
            {ok, _} = vonnegut_sup:create_topic(Topic),
            write(Sender, Topic, Partition, RecordSet)
    end.

ack(Topic, Partition, LatestId) ->
    vg_pending_writes:ack(Topic, Partition, LatestId).

init([Topic, Partition, NextServer, Tab]) ->
    Config = setup_config(),
    Partition = 0,
    LogDir = Config#config.log_dir,
    TopicDir = filename:join(LogDir, [binary_to_list(Topic), "-", integer_to_list(Partition)]),
    filelib:ensure_dir(filename:join(TopicDir, "ensure")),

    {Id, LatestIndex, LatestLog} = find_latest_id(TopicDir, Topic, Partition),
    LastLogId = filename:basename(LatestLog, ".log"),
    {ok, LogFD} = vg_utils:open_append(LatestLog),
    {ok, IndexFD} = vg_utils:open_append(LatestIndex),

    {ok, Position} = file:position(LogFD, eof),
    {ok, IndexPosition} = file:position(IndexFD, eof),

    %% Trigger server start on next server
    NextBrick =
        case NextServer of
            solo -> solo;
            tail -> tail;
            last -> last;
            S when is_atom(S) ->
                lager:info("at=segment_chain_init topic=~p partition=~p next=~p", [Topic, Partition, NextServer]),
                {ok, SupPid} = vonnegut_sup:create_topic(S, Topic, [Partition]),
                Children = supervisor:which_children(SupPid),
                [{_, Pid, _, _}] = lists:filter(fun({{T, P}, _, _, _}) ->
                                                        T == Topic andalso P == Partition;
                                                   (_) -> false
                                                end, Children),
                Pid
        end,

    {ok, #state{id=Id,
                next_brick=NextBrick,
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

handle_call({write, RecordSet}, _From, State=#state{topic=_Topic,
                                                     partition=_Partition,
                                                     next_brick=NextBrick}) ->
    {FirstID, State1} = write_record_set(RecordSet, State),
    case NextBrick of
        solo -> ok;
        tail -> ok;
        last -> ok;
        _ ->
            gen_server:cast(NextBrick, {write, self(), RecordSet})
    end,
    {reply, {ok, FirstID}, State1};
handle_call(_Msg, _From, State) ->
    lager:info("bad call ~p ~p", [_Msg, _From]),
    {noreply, State}.

handle_cast({write, Sender, RecordSet}, State=#state{topic=_Topic,
                                                      partition=_Partition,
                                                      next_brick=NextBrick}) ->
    {_FirstID, State1} = write_record_set(RecordSet, State),
    case NextBrick of
        solo -> ok;
        tail -> ok;
        last -> ok;
        _ ->
            gen_server:cast(NextBrick, {write, self(), RecordSet})
    end,

    %% TODO: batch acks
    Sender ! {ack, State1#state.id},

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
    {State#state.id, lists:foldl(fun(Record, StateAcc=#state{id=Id,
                                                              byte_count=ByteCount}) ->
                                     {NextId, Size, Bytes} = vg_protocol:encode_log(Id, Record),
                                     StateAcc1 = #state{pos=Position1} = maybe_roll(Size, StateAcc),
                                     update_log(Bytes, StateAcc1),
                                     StateAcc2 = StateAcc1#state{byte_count=ByteCount+Size},
                                     StateAcc3 = update_index(StateAcc2),
                                     case Next of
                                         solo -> ok;
                                         last -> ok;
                                         tail -> ok;
                                         _ ->
                                             vg_pending_writes:add(Tab, Id, Bytes)
                                     end,

                                     StateAcc3#state{id=NextId,
                                                     pos=Position1+Size}
                                 end, State, RecordSet)}.

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
    ok = file:close(LogFile),
    ok = file:close(IndexFile),

    {NewIndexFile, NewLogFile} = new_index_log_files(TopicDir, Id),
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

%% Rolling log and index files, so open new empty ones for appending
new_index_log_files(TopicDir, Id) ->
    IndexFilename = vg_utils:index_file(TopicDir, Id),
    LogFilename = vg_utils:log_file(TopicDir, Id),

    lager:debug("opening new log files: ~p ~p ~p", [Id, IndexFilename, LogFilename]),
    %% Make sure empty?
    {ok, IndexFile} = vg_utils:open_append(IndexFilename),
    {ok, LogFile} = vg_utils:open_append(LogFilename),
    {IndexFile, LogFile}.

find_latest_id(TopicDir, Topic, Partition) ->
    SegmentId = vg_log_segments:find_active_segment(Topic, Partition),
    IndexFilename = vg_utils:index_file(TopicDir, SegmentId),
    {Offset, Position} = last_in_index(TopicDir, IndexFilename, SegmentId),
    LogSegmentFilename = vg_utils:log_file(TopicDir, SegmentId),
    {ok, Log} = vg_utils:open_read(LogSegmentFilename),
    try
        NewId = find_last_log(Log, Offset, file:pread(Log, Position, 16)),
        {NewId+SegmentId, IndexFilename, LogSegmentFilename}
    after
        file:close(Log)
    end.

last_in_index(TopicDir, IndexFilename, SegmentId) ->
    case vg_utils:open_read(IndexFilename) of
        {error, enoent} when SegmentId =:= 0 ->
            %% Index file doesn't exist, if this is the first segment (0)
            %% we can just create the files assuming this is a topic creation.
            %% Will fail if an empty topic-partition dir exists on boot since
            %% vg_topic_sup will not be started yet.
            {NewIndexFile, NewLogFile} = new_index_log_files(TopicDir, SegmentId),
            file:close(NewIndexFile),
            file:close(NewLogFile),
            {0, 0};
        {ok, Index} ->
            try
                case file:pread(Index, {eof, 6}, 6) of
                    {ok, <<Offset:24/signed, Position:24/signed>>} ->
                        {Offset, Position};
                    _ ->
                        {0, 0}
                end
            after
                file:close(Index)
            end
    end.

%% Find the Id for the last log in the log file Log
find_last_log(Log, _, {ok, <<NewId:64/signed, Size:32/signed>>}) ->
    case file:read(Log, Size + 12) of
        {ok, <<_:Size/binary, Data:12/binary>>} ->
            find_last_log(Log, NewId, {ok, Data});
        _ ->
            NewId+1
    end;
find_last_log(_Log, Id, _) ->
    Id.

setup_config() ->
    {ok, [LogDir]} = application:get_env(vonnegut, log_dirs),
    {ok, SegmentBytes} = application:get_env(vonnegut, segment_bytes),
    {ok, IndexMaxBytes} = application:get_env(vonnegut, index_max_bytes),
    {ok, IndexIntervalBytes} = application:get_env(vonnegut, index_interval_bytes),
    #config{log_dir=LogDir,
            segment_bytes=SegmentBytes,
            index_max_bytes=IndexMaxBytes,
            index_interval_bytes=IndexIntervalBytes}.

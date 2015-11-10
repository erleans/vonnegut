-module(vg_log).
-behaviour(gen_server).

-export([start_link/2,
         write/3,
         find_in_log/3]).

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
                byte_count  :: integer(),
                pos         :: integer(),
                index_pos   :: integer(),
                log_fd      :: file:fd(),
                base_offset :: integer(),
                segment_id  :: integer(),
                index_fd    :: file:fd(),

                config      :: #config{}
               }).

start_link(Topic, Partition) ->
    gen_server:start_link({via, gproc, {n,l,{?MODULE,Topic,Partition}}}, ?MODULE, [Topic, Partition], []).

-spec write(Topic, Partition, Message) -> ok when
      Topic :: binary(),
      Partition :: integer(),
      Message :: binary().
write(Topic, Partition, Message) ->
    gen_server:call({via, gproc, {n,l,{?MODULE, Topic, Partition}}}, {write, Message}).

init([Topic, Partition]) ->
    Config = setup_config(),

    LogDir = Config#config.log_dir,
    TopicDir = filename:join(LogDir, <<Topic/binary, "-", Partition/binary>>),
    filelib:ensure_dir(filename:join(TopicDir, "ensure")),

    {Id, LatestIndex, LatestLog} = find_latest_id(TopicDir),
    LastLogId = filename:basename(LatestLog, ".log"),
    {ok, LogFD} = file:open(LatestLog, [append, raw]),
    {ok, IndexFD} = file:open(LatestIndex, [append, raw]),

    {ok, Position} = file:position(LogFD, eof),
    {ok, IndexPosition} = file:position(IndexFD, eof),

    {ok, #state{id=Id,
                topic_dir=TopicDir,
                byte_count=0,
                pos=Position,
                index_pos=IndexPosition,
                log_fd=LogFD,
                segment_id=0,
                base_offset=list_to_integer(LastLogId),
                index_fd=IndexFD,

                config=Config
               }}.

handle_call({write, Message}, _From, State) ->
    State1 = write_message(Message, State),
    {reply, ok, State1}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

%% Internal functions

write_message(Message, State=#state{id=Id,
                                    pos=Position,
                                    index_pos=IndexPosition,
                                    byte_count=ByteCount}) ->
    Bytes = vg_encode:log(Id, Message),
    Size = erlang:iolist_size(Bytes),
    State1 = maybe_roll(Size, State),
    update_log(Bytes, State1),
    update_index(State1),
    State1#state{id=Id+1,
                 pos=Position+Size,
                 index_pos=IndexPosition+16,
                 byte_count=ByteCount+Size}.

%% Create new log segment and index file if current segment is too large
%% or if the index file is over its max and would be written to again.
maybe_roll(Size, State=#state{id=Id,
                              topic_dir=TopicDir,
                              log_fd=LogFile,
                              index_fd=IndexFile,
                              pos=Position,
                              byte_count=ByteCount,
                              index_pos=IndexPosition,
                              config=#config{segment_bytes=SegmentBytes,
                                             index_max_bytes=IndexMaxBytes,
                                             index_interval_bytes=IndexIntervalBytes}})
  when Position+Size > SegmentBytes
     ; ByteCount+Size >= IndexIntervalBytes
       , IndexPosition+6 > IndexMaxBytes ->
    ok = file:close(LogFile),
    ok = file:close(IndexFile),

    {NewIndexFile, NewLogFile} = new_index_log_files(TopicDir, Id),

    State#state{log_fd=NewLogFile,
                index_fd=NewIndexFile,
                byte_count=0,
                pos=0,
                index_pos=0};
maybe_roll(_, State) ->
    State.

%% Append encoded log to segment
update_log(Message, #state{log_fd=LogFile}) ->
    ok = file:write(LogFile, Message).

%% Add to index if the number of bytes written to the log since the last index record was written
update_index(State=#state{id=Id,
                          pos=Position,
                          index_fd=IndexFile,
                          byte_count=ByteCount,
                          index_pos=IndexPosition,
                          base_offset=BaseOffset,
                          config=#config{index_interval_bytes=IndexIntervalBytes}})
  when ByteCount >= IndexIntervalBytes ->
    IndexEntry = <<(Id - BaseOffset):24/signed, Position:24/signed>>,
    ok = file:write(IndexFile, IndexEntry),
    State#state{index_pos=IndexPosition+6};
update_index(State) ->
    State.

%% Rolling log and index files, so open new empty ones for appending
new_index_log_files(TopicDir, Id) ->
    IndexFilename = vg_utils:index_file(TopicDir, Id),
    LogFilename = vg_utils:log_file(TopicDir, Id),

    %% Make sure empty?
    {ok, IndexFile} = file:open(IndexFilename, [append, raw]),
    {ok, LogFile} = file:open(LogFilename, [append, raw]),
    {IndexFile, LogFile}.

%% Functions

find_latest_id(TopicDirBinary) ->
    TopicDir = binary_to_list(TopicDirBinary),
    case lists:reverse(lists:sort(filelib:wildcard(filename:join(TopicDir, "*.log")))) of
        [] ->
            LatestIndex = vg_utils:index_file(TopicDir, 0),
            LatestLog = vg_utils:log_file(TopicDir, 0),
            {0, LatestIndex, LatestLog};
        [LatestLog | _] ->
            {ok, Index} = case lists:reverse(lists:sort(filelib:wildcard(filename:join(TopicDir, "*.index")))) of
                              [] ->
                                  LatestIndex = vg_utils:index_file(TopicDir, 0),
                                  {ok, <<>>};
                              [LatestIndex | _] ->
                                  file:read_file(LatestIndex)
                          end,
            BaseOffset = list_to_integer(filename:basename(LatestIndex, ".index")),
            {Offset, Position} = latest_in_index(Index),
            {ok, Log} = file:open(LatestLog, [read, raw, binary]),
            try
                NewId = find_last_log(Log, Offset, file:pread(Log, Position, 16)),
                file:close(Log),
                {NewId+BaseOffset, LatestIndex, LatestLog}
            after
                file:close(Log)
            end
    end.

%% Find the last Id (Offset - BaseOffset) and file position in the index
latest_in_index(<<>>) ->
    {0, 0};
latest_in_index(<<Id:24/signed, Position:24/signed>>) ->
    {Id, Position};
latest_in_index(<<_Id:24/signed, _Position:24/signed, Rest/binary>>) ->
    latest_in_index(Rest).

%% Find the Id for the last log in the log file Log
find_last_log(Log, _, {ok, <<NewId:64/signed, Size:32/signed, _CRC:32/signed>>}) ->
    MessageSize = Size - 4,
    case file:read(Log, MessageSize + 16) of
        {ok, <<_:MessageSize/binary, Data:16/binary>>} ->
            find_last_log(Log, NewId, {ok, Data});
        _ ->
            NewId
    end;
find_last_log(_Log, Id, _) ->
    Id.

%% Find the position in Log file of the start of a log with id Id
-spec find_in_log(Log, Id, Position) -> integer() when
      Log :: file:fd(),
      Id :: integer(),
      Position :: integer().
find_in_log(Log, Id, Position) ->
    {ok, _} = file:position(Log, Position),
    find_in_log(Log, Id, Position, file:read(Log, 16)).

find_in_log(_Log, Id, Position, {ok, <<Id:64/signed, _Size:32/signed, _CRC:32/signed>>}) ->
    Position;
find_in_log(Log, Id, Position, {ok, <<_NewId:64/signed, Size:32/signed, _CRC:32/signed>>}) ->
    MessageSize = Size - 4,
    case file:read(Log, MessageSize + 16) of
        {ok, <<M:MessageSize/binary, Data:16/binary>>} ->
            find_in_log(Log, Id, Position+MessageSize+16, {ok, Data});
        _ ->
            error
    end;
find_in_log(_, _, _, _) ->
    error.

setup_config() ->
    {ok, [LogDir]} = application:get_env(vonnegut, log_dirs),
    {ok, SegmentBytes} = application:get_env(vonnegut, segment_bytes),
    {ok, IndexMaxBytes} = application:get_env(vonnegut, index_max_bytes),
    {ok, IndexIntervalBytes} = application:get_env(vonnegut, index_interval_bytes),
    #config{log_dir=LogDir,
            segment_bytes=SegmentBytes,
            index_max_bytes=IndexMaxBytes,
            index_interval_bytes=IndexIntervalBytes}.

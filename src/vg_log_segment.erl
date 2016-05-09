%%
-module(vg_log_segment).
-behaviour(gen_server).

-export([start_link/3,
         find_message_offset/4,
         stop/3]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {topic_dir        :: file:filename(),
                segment_offset   :: integer(),
                log_segment_fd   :: file:fd(),
                index_segment_fd :: file:fd()
               }).

start_link(Topic, Partition, SegmentId) ->
    gen_server:start_link({via, gproc, {n,l,{?MODULE,Topic,Partition,SegmentId}}},
                          ?MODULE, [Topic, Partition, SegmentId], []).

-spec find_message_offset(Topic, Partition, SegmentId, MessageId) -> integer() when
      Topic     :: binary(),
      Partition :: integer(),
      SegmentId :: integer(),
      MessageId :: integer().
find_message_offset(Topic, Partition, SegmentId, MessageId) ->
    gen_server:call({via, gproc, {n,l,{?MODULE, Topic, Partition, SegmentId}}}, {find_message_offset, MessageId}).

-spec stop(Topic, Partition, SegmentId) -> ok when
      Topic     :: binary(),
      Partition :: integer(),
      SegmentId :: integer().
stop(Topic, Partition, SegmentId) ->
    gen_server:stop({via, gproc, {n,l,{?MODULE, Topic, Partition, SegmentId}}}).

init([Topic, Partition, SegmentId]) ->
    {ok, [LogDir]} = application:get_env(vonnegut, log_dirs),
    TopicDir = filename:join(LogDir, <<Topic/binary, "-", (ec_cnv:to_binary(Partition))/binary>>),
    LogSegmentFilename = vg_utils:log_file(TopicDir, SegmentId),
    IndexSegmentFilename = vg_utils:index_file(TopicDir, SegmentId),

    %% Open log and index segment files, advise the OS we'll be reading randomly from them
    {ok, LogSegmentFD} = vg_utils:open_read(LogSegmentFilename),
    file:advise(LogSegmentFD, 0, 0, random),
    {ok, IndexSegmentFD} = vg_utils:open_read(IndexSegmentFilename),
    file:advise(IndexSegmentFD, 0, 0, random),
    {ok, #state{topic_dir=TopicDir,
                segment_offset=SegmentId,
                log_segment_fd=LogSegmentFD,
                index_segment_fd=IndexSegmentFD}}.

handle_call({find_message_offset, MessageId}, _From, State=#state{segment_offset=SegmentOffset,
                                                                  log_segment_fd=LogSegmentFD,
                                                                  index_segment_fd=IndexSegmentFD}) ->
    InitialOffset = vg_index:find_in_index(IndexSegmentFD, SegmentOffset, MessageId),
    Position = find_in_log(LogSegmentFD, MessageId, InitialOffset),
    {reply, Position, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

%% Internal functions

%% Find the position in Log file of the start of a log with id Id
-spec find_in_log(Log, Id, Position) -> integer() when
      Log      :: file:fd(),
      Id       :: integer(),
      Position :: integer().
find_in_log(Log, Id, Position) ->
    {ok, _} = file:position(Log, Position),
    find_in_log(Log, Id, Position, file:read(Log, 12)).

find_in_log(_Log, Id, Position, {ok, <<Id:64/signed, _Size:32/signed>>}) ->
    Position;
find_in_log(Log, Id, Position, {ok, <<_:64/signed, Size:32/signed>>}) ->
    case file:read(Log, Size + 12) of
        {ok, <<_:Size/binary, Data:12/binary>>} ->
            find_in_log(Log, Id, Position+Size+12, {ok, Data});
        {ok, <<_:Size/binary>>} ->
            Position+Size+12;
        eof ->
            Position+Size+12
    end;
find_in_log(_, _, _, _) ->
    error.

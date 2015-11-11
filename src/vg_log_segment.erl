%%
-module(vg_log_segment).
-behaviour(gen_server).

-export([start_link/3,
         find_message_offset/5]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {topic_dir   :: file:filename(),
                segment_fd      :: file:fd()
               }).

start_link(Topic, Partition, SegmentId) ->
    gen_server:start_link({via, gproc, {n,l,{?MODULE,Topic,Partition,SegmentId}}},
                         ?MODULE, [Topic, Partition, SegmentId], []).

-spec find_message_offset(Topic, Partition, SegmentId, InitialOffset, MessageId) -> integer() when
      Topic :: binary(),
      Partition :: integer(),
      SegmentId :: integer(),
      InitialOffset :: integer(),
      MessageId :: integer().
find_message_offset(Topic, Partition, SegmentId, InitialOffset, MessageId) ->
    gen_server:call({via, gproc, {n,l,{?MODULE, Topic, Partition, SegmentId}}}, {find_message_offset, InitialOffset, MessageId}).

init([Topic, Partition, SegmentId]) ->
    {ok, [LogDir]} = application:get_env(vonnegut, log_dirs),
    TopicDir = filename:join(LogDir, <<Topic/binary, "-", Partition/binary>>),
    filelib:ensure_dir(filename:join(TopicDir, "ensure")),

    LogSegmentFilename = filename:basename(SegmentId, ".log"),
    {ok, LogSegmentFD} = vg_utils:open_read(LogSegmentFilename),
    file:advise(LogSegmentFD, 0, 0, random),
    {ok, #state{topic_dir=TopicDir,
                segment_fd=LogSegmentFD}}.

handle_call({find_message_offset, InitialOffset, MessageId}, _From, State=#state{segment_fd=LogSegmentFD}) ->
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
        {ok, <<_:MessageSize/binary, Data:16/binary>>} ->
            find_in_log(Log, Id, Position+MessageSize+16, {ok, Data});
        _ ->
            error
    end;
find_in_log(_, _, _, _) ->
    error.

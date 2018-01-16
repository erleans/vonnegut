%%
-module(vg_log_segments).

-export([init_table/0,
         load_all/2,
         insert/3,
         local/2,
         find_log_segment/3,
         find_active_segment/2,
         find_segment_offset/3,
         find_record_offset/4,
         new_index_log_files/2,
         find_latest_id/3,

         %% for testing
         last_in_index/3]).

-include("vg.hrl").

-define(LOG_SEGMENT_MATCH_PATTERN(Topic, Partition), {Topic,Partition,'$1'}).
-define(LOG_SEGMENT_GUARD(RecordId), [{is_integer, '$1'}, {'=<', '$1', RecordId}]).
-define(LOG_SEGMENT_RETURN, ['$1']).

init_table() ->
    ets:new(?SEGMENTS_TABLE, [bag, public, named_table, {read_concurrency, true}]).

load_all(Topic, Partition) ->
    TopicDir = vg_utils:topic_dir(Topic, Partition),
    case filelib:wildcard(filename:join(TopicDir, "*.log")) of
        [] ->
            insert(Topic, Partition, 0),
            vg_topics:insert_hwm(Topic, Partition, 0);
        LogSegments ->
            [begin
                 SegmentId = list_to_integer(filename:basename(LogSegment, ".log")),
                 insert(Topic, Partition, SegmentId)
             end || LogSegment <- LogSegments],
            {HWM, _, _} = find_latest_id(TopicDir, Topic, Partition),
            vg_topics:insert_hwm(Topic, Partition, HWM)
    end.

insert(Topic, Partition, SegmentId) ->
    prometheus_gauge:inc(log_segments, [Topic]),
    ets:insert(?SEGMENTS_TABLE, {Topic, Partition, SegmentId}).

local(Topic, Partition) ->
    case ets:select(?SEGMENTS_TABLE, [{?LOG_SEGMENT_MATCH_PATTERN(Topic, Partition),
                                       ?LOG_SEGMENT_GUARD(0),
                                       ?LOG_SEGMENT_RETURN}]) of
        [] -> false;
        _ -> true
    end.

-spec find_log_segment(Topic, Partition, RecordId) -> integer() when
      Topic     :: binary(),
      Partition :: integer(),
      RecordId :: integer().
find_log_segment(Topic, Partition, RecordId) ->
    %% Find all registered log segments for topic-partition < the recordid we are looking for
    case ets:select(?SEGMENTS_TABLE, [{?LOG_SEGMENT_MATCH_PATTERN(Topic, Partition),
                                       ?LOG_SEGMENT_GUARD(RecordId),
                                       ?LOG_SEGMENT_RETURN}]) of
        [] ->
            0;
        Matches  ->
            %% Return largest, being the largest log segment
            %% offset that is still less than the record offset
            lists:max(Matches)
    end.

-spec find_active_segment(Topic, Partition) -> integer() when
      Topic     :: binary(),
      Partition :: integer().
find_active_segment(Topic, Partition) ->
    case ets:select(?SEGMENTS_TABLE, [{?LOG_SEGMENT_MATCH_PATTERN(Topic, Partition),
                                       [],
                                       ?LOG_SEGMENT_RETURN}]) of
        [] ->
            0;
        Matches  ->
            lists:max(Matches)
    end.

-spec find_segment_offset(Topic, Partition, RecordId) -> {integer(), integer()} when
      Topic     :: binary(),
      Partition :: integer(),
      RecordId :: integer().
find_segment_offset(_Topic, _Partition, 0) ->
    {0, 0};
find_segment_offset(Topic, Partition, RecordId) when RecordId >= 0 ->
    SegmentId = find_log_segment(Topic, Partition, RecordId),
    {SegmentId, find_record_offset(Topic, Partition, SegmentId, RecordId)}.

-spec find_record_offset(Topic, Partition, SegmentId, RecordId) -> integer() when
      Topic     :: binary(),
      Partition :: integer(),
      SegmentId :: integer(),
      RecordId :: integer().
find_record_offset(Topic, Partition, SegmentId, RecordId) ->
    TopicDir = vg_utils:topic_dir(Topic, Partition),
    LogSegmentFilename = vg_utils:log_file(TopicDir, SegmentId),
    IndexSegmentFilename = vg_utils:index_file(TopicDir, SegmentId),

    %% Open log and index segment files, advise the OS we'll be reading randomly from them
    {ok, LogSegmentFD} = vg_utils:open_read(LogSegmentFilename),
    file:advise(LogSegmentFD, 0, 0, random),
    {ok, IndexSegmentFD} = vg_utils:open_read(IndexSegmentFilename),
    file:advise(IndexSegmentFD, 0, 0, random),

    try
        InitialOffset = vg_index:find_in_index(IndexSegmentFD, SegmentId, RecordId),
        lager:info("InitialOffset topic=~p segment_id=~p initial_offset=~p", [Topic, SegmentId, InitialOffset]),
        find_in_log(LogSegmentFD, RecordId, InitialOffset)
    after
        file:close(LogSegmentFD),
        file:close(IndexSegmentFD)
    end.

%% Find the position in Log file of the start of a log with id Id
-spec find_in_log(Log, Id, Position) -> integer() when
      Log      :: file:fd(),
      Id       :: integer(),
      Position :: integer().
find_in_log(Log, Id, Position) ->
    {ok, _} = file:position(Log, Position),
    find_in_log(Log, Id, Position, file:read(Log, 12)).

find_in_log(Log, Id, _Position, {ok, <<FileId:64/signed, _Size:32/signed>>}) when FileId > Id ->
    %% we'll never succeed if this is the case, start scan over.
    file:position(Log, 0),
    find_in_log(Log, Id, 0, file:read(Log, 12));
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
    0.

find_latest_id(TopicDir, Topic, Partition) ->
    SegmentId = vg_log_segments:find_active_segment(Topic, Partition),
    IndexFilename = vg_utils:index_file(TopicDir, SegmentId),
    {Offset, Position} = last_in_index(TopicDir, IndexFilename, SegmentId),
    LogSegmentFilename = vg_utils:log_file(TopicDir, SegmentId),
    {ok, Log} = vg_utils:open_read(LogSegmentFilename),
    try
        file:position(Log, Position),
        NewId = find_last_log(Log, Offset, file:read(Log, 12)),
        {NewId+SegmentId, IndexFilename, LogSegmentFilename}
    after
        file:close(Log)
    end.

%% Rolling log and index files, so open new empty ones for appending
new_index_log_files(TopicDir, Id) ->
    IndexFilename = vg_utils:index_file(TopicDir, Id),
    LogFilename = vg_utils:log_file(TopicDir, Id),

    lager:debug("opening new log files: ~p ~p ~p", [Id, IndexFilename, LogFilename]),
    %% Make sure empty?
    {ok, IndexFile} = vg_utils:open_append(IndexFilename),
    {ok, LogFile} = vg_utils:open_append(LogFilename),
    {IndexFile, LogFile}.


last_in_index(TopicDir, IndexFilename, SegmentId) ->
    case file:open(IndexFilename, [read, binary]) of
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
                case file:pread(Index, {eof, -6}, 6) of
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
        {ok, <<_:Size/bytes, Data:12/bytes>>} ->
            find_last_log(Log, NewId, {ok, Data});
        _ ->
            NewId + 1
    end;
find_last_log(_Log, Id, _) ->
    Id.

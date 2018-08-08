%%
-module(vg_log_segments).

-export([init_table/0,
         load_existing/2,
         load_all/2,
         delete_segments/2,
         delete_indexes/2,
         regenerate_indexes/2,
         cleanup_segments_table/2,
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

load_existing(Topic, Partition) ->
    TopicDir = vg_utils:topic_dir(Topic, Partition),
    case filelib:wildcard(filename:join(TopicDir, "*.log")) of
        [] ->
            throw({topic_not_found, Topic, Partition});
        LogSegments ->
            load_segments(Topic, Partition, LogSegments)
    end.

load_all(Topic, Partition) ->
    TopicDir = vg_utils:topic_dir(Topic, Partition),
    case filelib:wildcard(filename:join(TopicDir, "*.log")) of
        [] ->
            insert(Topic, Partition, 0),
            vg_topics:insert_hwm(Topic, Partition, 0),
            [];
        LogSegments ->
            load_segments(Topic, Partition, LogSegments)
    end.

load_segments(Topic, Partition, LogSegments) ->
    [begin
         SegmentId = list_to_integer(filename:basename(LogSegment, ".log")),
         insert(Topic, Partition, SegmentId),
         SegmentId
     end || LogSegment <- LogSegments].

delete_segments(Topic, Partition) ->
    TopicDir = vg_utils:topic_dir(Topic, Partition),
    AllFiles = filelib:wildcard(filename:join(TopicDir, "*")),
    ok = lists:foreach(fun file:delete/1, AllFiles),
    file:del_dir(TopicDir),
    ok.

delete_indexes(Topic, Partition) ->
    TopicDir = vg_utils:topic_dir(Topic, Partition),
    AllFiles = filelib:wildcard(filename:join(TopicDir, "*.index")),
    ok = lists:foreach(fun file:delete/1, AllFiles).

regenerate_indexes(Topic, Partition) ->
    TopicDir = vg_utils:topic_dir(Topic, Partition),
    AllFiles = filelib:wildcard(filename:join(TopicDir, "*.log")),
    ok = lists:foreach(fun regenerate_index/1, AllFiles).

regenerate_index(LogFilename) ->
    TopicDir = filename:dirname(LogFilename),
    StrID = filename:basename(LogFilename, ".log"),
    ID = list_to_integer(StrID),
    IndexFilename = vg_utils:index_file(TopicDir, ID),
    {ok, IndexFile} = vg_utils:open_append(IndexFilename),
    {ok, LogFile} = vg_utils:open_read(LogFilename),

    %% ignore index_max_bytes because it makes no sense without the
    %% ability to rewrite the segments
    {ok, IndexInterval} = application:get_env(vonnegut, index_interval_bytes),
    regen(file:pread(LogFile, 0, ?OFFSET_AND_LENGTH_BYTES), 0, LogFile, ID, IndexFile, 99999999, IndexInterval).

regen(eof, _Location, Log, _ID, Index, _Bytes, _IndexInterval) ->
    file:close(Log),
    file:close(Index),
    ok;
regen({ok, <<Offset:64/signed, Size:32/signed>>}, Location, Log, BaseOffset, Index, Bytes,
      IndexInterval) ->
    TotalSize = Size + ?OFFSET_AND_LENGTH_BYTES,
    NextLocation = Location + TotalSize,
    NewBytes =
        case Bytes + TotalSize >= IndexInterval of
            true ->
                IndexEntry = <<(Offset - BaseOffset):?INDEX_OFFSET_BITS/unsigned,
                               Location:?INDEX_OFFSET_BITS/unsigned>>,
                ok = file:write(Index, IndexEntry),
                0;
            _ ->
                Bytes + TotalSize
        end,
    regen(file:pread(Log, NextLocation, ?OFFSET_AND_LENGTH_BYTES), NextLocation, Log, BaseOffset, Index, NewBytes,
          IndexInterval).

cleanup_segments_table(Topic, Partition) ->
    NumDeleted = ets:select_delete(?SEGMENTS_TABLE,
                                   [{?LOG_SEGMENT_MATCH_PATTERN(Topic, Partition),
                                     [],
                                     ?LOG_SEGMENT_RETURN}]),
    lager:info("deleted ~p segments from the table", [NumDeleted]),
    prometheus_gauge:dec(log_segments, [NumDeleted]).

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
    case find_log_segment_(Topic, Partition, RecordId) of
        [] ->
            %% load from disk and try again
            load_existing(Topic, Partition),
            find_log_segment_(Topic, Partition, RecordId);
        Match ->
            %% Return largest, being the largest log segment
            %% offset that is still less than the record offset
            Match
    end.

%% internal version that won't try again if no match found
find_log_segment_(Topic, Partition, RecordId) ->
    %% Find all registered log segments for topic-partition < the recordid we are looking for
    case ets:select(?SEGMENTS_TABLE, [{?LOG_SEGMENT_MATCH_PATTERN(Topic, Partition),
                                       ?LOG_SEGMENT_GUARD(RecordId),
                                       ?LOG_SEGMENT_RETURN}]) of
        [] ->
            [];
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
            %% check disk
            case load_existing(Topic, Partition) of
                [] ->
                    0;
                Segments ->
                    lists:max(Segments)
            end;
        Matches  ->
            lists:max(Matches)
    end.

-spec find_segment_offset(Topic, Partition, RecordId) -> {integer(), {integer(), integer()}} when
      Topic     :: binary(),
      Partition :: integer(),
      RecordId :: integer().
find_segment_offset(_Topic, _Partition, 0) ->
    {0, {0, 0}};
find_segment_offset(Topic, Partition, RecordId) when RecordId >= 0 ->
    SegmentId = find_log_segment(Topic, Partition, RecordId),
    {SegmentId, find_record_offset(Topic, Partition, SegmentId, RecordId)}.

-spec find_record_offset(Topic, Partition, SegmentId, RecordId) -> {integer(), integer()} when
      Topic     :: binary(),
      Partition :: integer(),
      SegmentId :: integer(),
      RecordId :: integer().
find_record_offset(Topic, Partition, SegmentId, RecordId) ->
    TopicDir = vg_utils:topic_dir(Topic, Partition),
    LogSegmentFilename = vg_utils:log_file(TopicDir, SegmentId),
    IndexSegmentFilename = vg_utils:index_file(TopicDir, SegmentId),

    %% Open log and index segment files, advise the OS we'll be reading randomly from them
    case vg_utils:open_read(LogSegmentFilename) of
        {ok, LogSegmentFD} ->
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
            end;
        {error, enoent} ->
            throw({topic_not_found, Topic, Partition})
    end.

%% Find the position in Log file of the start of a log with id Id
-spec find_in_log(Log, Id, Position) -> {integer(), integer()} when
      Log      :: file:fd(),
      Id       :: integer(),
      Position :: integer().
find_in_log(Log, Id, Position) ->
    {ok, _} = file:position(Log, Position),
    find_in_log(Log, Id, Position, 0, file:read(Log, ?OFFSET_AND_LENGTH_BYTES)).

find_in_log(_Log, Id, Position, LastSize, {ok, <<FileId:64/signed, _Size:32/signed>>}) when FileId > Id ->
    {Position, LastSize};
find_in_log(_Log, Id, Position, LastSize, {ok, <<Id:64/signed, _Size:32/signed>>}) ->
    {Position+LastSize, 0};
find_in_log(Log, Id, Position, LastSize, {ok, <<FileId:64/signed, Size:32/signed>>}) ->
    case file:read(Log, Size + ?OFFSET_AND_LENGTH_BYTES) of
        {ok, <<_:Size/binary, Data:?OFFSET_AND_LENGTH_BYTES/binary>>} ->
            find_in_log(Log, Id, Position+LastSize, Size+?OFFSET_AND_LENGTH_BYTES, {ok, Data});
        {ok, <<D:Size/binary>>} ->
            case D of
                <<_LeaderEpoch:32/signed-integer,
                  ?MAGIC_TWO:8/signed-integer,
                  _CRC:32/signed-integer,
                  _Attributes:16/signed-integer,
                  LastOffsetDelta:32/signed-integer, _/binary>> when LastOffsetDelta + FileId >= Id ->
                    {Position+LastSize, Size+?OFFSET_AND_LENGTH_BYTES};
                _ ->
                    {Position+LastSize+Size+?OFFSET_AND_LENGTH_BYTES, 0}
            end;
        eof ->
            {Position+LastSize, Size+?OFFSET_AND_LENGTH_BYTES}
    end;
find_in_log(_Log, _Id, Position, LastSize, _) ->
    {Position+LastSize, 0}.

find_latest_id(TopicDir, Topic, Partition) ->
    SegmentId = vg_log_segments:find_active_segment(Topic, Partition),
    IndexFilename = vg_utils:index_file(TopicDir, SegmentId),
    {Offset, Position} = last_in_index(TopicDir, IndexFilename, SegmentId),
    LogSegmentFilename = vg_utils:log_file(TopicDir, SegmentId),
    {ok, Log} = vg_utils:open_read(LogSegmentFilename),
    try
        file:position(Log, Position),
        NewId = find_last_log(Log, Offset, file:read(Log, ?OFFSET_AND_LENGTH_BYTES)),
        {NewId, IndexFilename, LogSegmentFilename}
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

%% consider moving this to vg_index, but then we might need to figure
%% out some other, cleaner way to do the create new case
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
            {-1, 0};
        {ok, Index} ->
            try
                case file:pread(Index, {eof, -?INDEX_ENTRY_SIZE}, ?INDEX_ENTRY_SIZE) of
                    {ok, <<Offset:?INDEX_OFFSET_BITS/signed, Position:?INDEX_POS_BITS/signed>>} ->
                        %% index stores offsets as offset from SegmentId
                        %% so add SegmentId here to get the real id
                        {Offset+SegmentId, Position};
                    _ ->
                        {-1, 0}
                end
            after
                file:close(Index)
            end
    end.

%% Find the Id for the last log in the log file Log
find_last_log(Log, _, {ok, <<NewId:64/signed, Size:32/signed>>}) ->
    case file:read(Log, Size + ?OFFSET_AND_LENGTH_BYTES) of
        {ok, <<Batch:Size/bytes, Data:?OFFSET_AND_LENGTH_BYTES/bytes>>} ->
            LastOffsetDelta = vg_protocol:last_offset_delta(Batch),
            find_last_log(Log, NewId+LastOffsetDelta, {ok, Data});
        {ok, <<Batch:Size/bytes, _/binary>>} ->
            LastOffsetDelta = vg_protocol:last_offset_delta(Batch),
            NewId + LastOffsetDelta
    end;
find_last_log(_Log, Id, _) ->
    Id.


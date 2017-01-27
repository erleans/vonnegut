%%
-module(vg_log_segments).

-export([init_table/0,
         load_all/2,
         insert/3,
         find_log_segment/3,
         find_active_segment/2,
         find_segment_offset/3,
         find_record_offset/4]).

-include("vg.hrl").

%% log segment servers are index as {Topic,Partition,SegmentId}
-define(LOG_SEGMENT_MATCH_PATTERN(Topic, Partition), {Topic,Partition,'$1'}).
-define(LOG_SEGMENT_GUARD(RecordId), [{is_integer, '$1'}, {'=<', '$1', RecordId}]).
-define(LOG_SEGMENT_RETURN, ['$1']).

init_table() ->
    ets:new(?SEGMENTS_TABLE, [bag, public, named_table, {read_concurrency, true}]).

load_all(Topic, Partition) ->
    TopicDir = vg_utils:topic_dir(Topic, Partition),
    case filelib:wildcard(filename:join(TopicDir, "*.log")) of
        [] ->
            insert(Topic, Partition, 0);
        LogSegments ->
            [insert(Topic, Partition, list_to_integer(filename:basename(LogSegment, ".log")))
            || LogSegment <- LogSegments]
    end.

insert(Topic, Partition, SegmentId) ->
    ets:insert(?SEGMENTS_TABLE, {Topic, Partition, SegmentId}).

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
find_segment_offset(Topic, Partition, RecordId) ->
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

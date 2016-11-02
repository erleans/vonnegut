-module(vg_utils).

-export([find_log_segment/3,
         find_active_segment/2,
         find_segment_offset/3,
         index_file/2,
         log_file/2,
         open_append/1,
         open_read/1]).

%% log segment servers are registered as {vg_log_segment,Topic,Partition,SegmentId}
-define(LOG_SEGMENT_MATCH_PATTERN(Topic, Partition), {{n,l,{vg_log_segment,Topic,Partition,'$1'}},'$2','$3'}).
-define(LOG_SEGMENT_GUARD(MessageId), [{is_integer, '$1'}, {'<', '$1', MessageId}]).
-define(LOG_SEGMENT_RETURN, ['$1']).

-spec find_log_segment(Topic, Partition, MessageId) -> integer() when
      Topic     :: binary(),
      Partition :: integer(),
      MessageId :: integer().
find_log_segment(Topic, Partition, MessageId) ->
    %% Find all registered log segments for topic-partition < the messageid we are looking for
    case gproc:select({l,n}, [{?LOG_SEGMENT_MATCH_PATTERN(Topic, Partition),
                               ?LOG_SEGMENT_GUARD(MessageId),
                               ?LOG_SEGMENT_RETURN}]) of
        [] ->
            0;
        Matches  ->
            %% Return largest, being the largest log segment
            %% offset that is still less than the message offset
            lists:max(Matches)
    end.

-spec find_active_segment(Topic, Partition) -> integer() when
      Topic     :: binary(),
      Partition :: integer().
find_active_segment(Topic, Partition) ->
    %% Find all registered log segments for topic-partition < the messageid we are looking for
    case gproc:select({l,n}, [{?LOG_SEGMENT_MATCH_PATTERN(Topic, Partition),
                              [],
                              ?LOG_SEGMENT_RETURN}]) of
        [] ->
            0;
        Matches  ->
            lists:max(Matches)
    end.

-spec find_segment_offset(Topic, Partition, MessageId) -> {integer(), integer()} when
      Topic     :: binary(),
      Partition :: integer(),
      MessageId :: integer().
find_segment_offset(Topic, Partition, MessageId) ->
    SegmentId = find_log_segment(Topic, Partition, MessageId),
    {SegmentId, vg_log_segment:find_message_offset(Topic, Partition, SegmentId, MessageId)}.

%% Convenience functions for creating index and log file names
index_file(TopicDir, Id) ->
    filename:join(TopicDir, io_lib:format("~20.10.0b.index", [Id])).

log_file(TopicDir, Id) ->
    filename:join(TopicDir, io_lib:format("~20.10.0b.log", [Id])).

open_append(Filename) ->
    case application:get_env(vonnegut, delayed_write) of
        {ok, true} ->
            %% Buffer writes up to DelayedWriteSize bytes or DelayMS milliseconds to save on OS calls
            {ok, DelayedWriteSize} = application:get_env(vonnegut, delayed_write_byte_size),
            {ok, DelayMS} = application:get_env(vonnegut, delayed_write_milliseconds),
            file:open(Filename, [append, raw, binary, {delayed_write, DelayedWriteSize, DelayMS}]);
        _ ->
            file:open(Filename, [append, raw, binary])
    end.

open_read(Filename) ->
    file:open(Filename, [read, raw, binary]).

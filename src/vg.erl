-module(vg).

-export([create_topic/1,
         ensure_topic/1,
         write/2,
         fetch/2,
         fetch/1]).

-include("vg.hrl").

-type record() :: #{id => integer(),
                    crc => integer(),
                    record := binary() | {binary(), binary()}}.
-type record_set() :: [record()].
-type topic() :: binary().

-export_types([topic/0,
               record/0,
               record_set/0]).

-spec create_topic(Topic :: topic()) -> ok.
create_topic(Topic) ->
    {ok, _} = vg_cluster_mgr:create_topic(Topic),
    ok.

-spec ensure_topic(Topic :: topic()) -> ok.
ensure_topic(Topic) ->
    case vg_cluster_mgr:create_topic(Topic) of
        {ok, _} ->
            ok;
        {error, exists} ->
            ok
    end.

-spec write(Topic, Record) -> ok | {error, any()} when
      Topic :: topic(),
      Record :: binary() | record_set().
write(Topic, Record) when is_binary(Record) ->
    vg_active_segment:write(Topic, 0, [#{record => Record}]);
write(Topic, RecordSet) when is_list(RecordSet) ->
    vg_active_segment:write(Topic, 0, RecordSet).

fetch(Topic) ->
    fetch(Topic, 0).

-spec fetch(Topic, Offset) -> RecordSet when
      Topic :: topic(),
      Offset :: integer(),
      RecordSet :: #{high_water_mark := integer(),
                      partition := 0,
                      record_set := record_set()}.
fetch(Topic, Offset) ->
    Partition = 0,
    {SegmentId, Position} = vg_log_segments:find_segment_offset(Topic, Partition, Offset),
    File = vg_utils:log_file(Topic, Partition, SegmentId),
    Size = filelib:file_size(File),
    {ok, Fd} = file:open(File, [read, binary, raw]),
    {ok, [Data]} = file:pread(Fd, [{Position, Size}]),
    file:close(Fd),
    #{high_water_mark => vg_topics:lookup_hwm(Topic, Partition),
      partition => 0,
      record_set => vg_protocol:decode_record_set(Data, [])}.

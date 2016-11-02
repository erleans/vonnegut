-module(vg).

-export([create_topic/1,
         write/2,
         get/2,
         get/1]).

create_topic(Topic) ->
    vg_topics:create_topic(Topic).

write(Topic, Message) when is_binary(Message) ->
    vg_log:write(Topic, 0, [Message]);
write(Topic, MessageSet) when is_list(MessageSet) ->
    vg_log:write(Topic, 0, MessageSet).

get(Topic) ->
    ?MODULE:get(Topic, 0).

get(Topic, MessageId) ->
    Partition = 0,
    {SegmentId, Position} = vg_utils:find_segment_offset(Topic, Partition, MessageId),
    File = vg_utils:log_file(Topic, Partition, SegmentId),
    Size = filelib:file_size(File),
    {ok, Fd} = file:open(File, [read, binary, raw]),
    {ok, [Data]} = file:pread(Fd, [{Position, Size}]),
    file:close(Fd),
    vg_encode:decode(Data).

-module(vg).

-export([create_topic/1,
         write/2,
         get/1]).

create_topic(Topic) ->
    vg_topics:create_topic(Topic).


write(Topic, Message) when is_binary(Message) ->
    vg_log:write(Topic, 0, [Message]);
write(Topic, MessageSet) when is_list(MessageSet) ->
    vg_log:write(Topic, 0, MessageSet).

get(Topic) ->
    vg_log:get(Topic, 0).

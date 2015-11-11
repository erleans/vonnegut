-module(vg).

-export([create_topic/1,
         write/2]).

create_topic(Topic) ->
    vonnegut_sup:create_topic(Topic).

write(Topic, Message) when is_binary(Message) ->
    vg_log:write(Topic, 0, [Message]);
write(Topic, MessageSet) when is_list(MessageSet) ->
    vg_log:write(Topic, 0, MessageSet).

-module(vg).

-export([create_topic/1,
         write/2]).

create_topic(Topic) ->
    vonnegut_sup:create_topic(Topic).

write(Topic, Message) ->
    vg_log:write(Topic, <<"0">>, Message).

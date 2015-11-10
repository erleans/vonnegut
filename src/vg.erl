-module(vg).

-export([create_topic/1]).

create_topic(Topic) ->
    vonnegut_sup:create_topic(Topic).

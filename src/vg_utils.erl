-module(vg_utils).

-export([index_file/2,
         log_file/2,
         open_append/1,
         open_read/1]).

%% Convenience functions for creating index and log file names
index_file(TopicDir, Id) ->
    filename:join(TopicDir, io_lib:format("~20.10.0b.index", [Id])).

log_file(TopicDir, Id) ->
    filename:join(TopicDir, io_lib:format("~20.10.0b.log", [Id])).

open_append(Filename) ->
    file:open(Filename, [append, raw, exclusive]). %% {delayed_write, Size, Delay}

open_read(Filename) ->
    file:open(Filename, [read, raw, binary]).

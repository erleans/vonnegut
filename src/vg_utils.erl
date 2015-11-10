-module(vg_utils).

-export([index_file/2,
         log_file/2]).

%% Convenience functions for creating index and log file names
index_file(TopicDir, Id) ->
    filename:join(TopicDir, io_lib:format("~20.10.0b.index", [Id])).

log_file(TopicDir, Id) ->
    filename:join(TopicDir, io_lib:format("~20.10.0b.log", [Id])).

-module(cleanup_SUITE).

-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() ->
    [delete_policy].

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, Config) ->
    Config.

delete_policy(_Config) ->
    ok.

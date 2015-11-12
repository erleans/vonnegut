-module(topic_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() ->
    [creation].

init_per_testcase(_, Config) ->
    PrivDir = ?config(priv_dir, Config),
    application:load(vonnegut),
    application:set_env(vonnegut, log_dirs, [filename:join(PrivDir, "data")]),
    application:ensure_all_started(vonnegut),
    Config.

end_per_testcase(_, Config) ->
    application:stop(vonnegut),
    Config.

creation(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Topic = vg_test_utils:create_random_name(<<"test_topic">>),
    TopicPartition = <<Topic/binary, "-0">>,
    TopicPartitionDir = binary_to_list(filename:join([PrivDir, "data", TopicPartition])),
    vg:create_topic(Topic),
    ?assert(filelib:is_dir(TopicPartitionDir)).

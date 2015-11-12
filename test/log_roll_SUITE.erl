-module(log_roll_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() ->
    [message_set_larger_than_max_segment].

init_per_testcase(_, Config) ->
    PrivDir = ?config(priv_dir, Config),
    application:load(vonnegut),
    application:set_env(vonnegut, log_dirs, [filename:join(PrivDir, "data")]),
    application:set_env(vonnegut, segment_bytes, 86),
    application:set_env(vonnegut, index_max_bytes, 18),
    application:set_env(vonnegut, index_interval_bytes, 24),
    application:ensure_all_started(vonnegut),
    crypto:start(),
    Config.

end_per_testcase(_, Config) ->
    Config.

message_set_larger_than_max_segment(Config) ->
    PrivDir = ?config(priv_dir, Config),
    Topic = vg_test_utils:create_random_name(<<"test_topic">>),
    TopicPartition = <<Topic/binary, "-0">>,
    TopicPartitionDir = binary_to_list(filename:join([PrivDir, "data", TopicPartition])),
    vg:create_topic(Topic),
    ?assert(filelib:is_dir(TopicPartitionDir)),

    vg:write(Topic, [crypto:rand_bytes(60)]),
    vg:write(Topic, [crypto:rand_bytes(60)]),

    %% Total size of a 60 byte message when written to log becomes 86 bytes
    %% Since index interval is 24 and 86 > 24, 1 index entry of 6 bytes should exist for each as well
    ?assertEqual(filelib:file_size(filename:join([TopicPartitionDir, "00000000000000000000.index"])), 6),
    ?assertEqual(filelib:file_size(filename:join([TopicPartitionDir, "00000000000000000000.log"])), 86),
    ?assertEqual(filelib:file_size(filename:join([TopicPartitionDir, "00000000000000000001.index"])), 6),
    ?assertEqual(filelib:file_size(filename:join([TopicPartitionDir, "00000000000000000001.log"])), 86).

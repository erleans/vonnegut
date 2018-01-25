-module(log_roll_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

-include("vg.hrl").

all() ->
    [record_set_larger_than_max_segment, regenerate_index_test].

init_per_testcase(regenerate_index_test, Config) ->
    PrivDir = ?config(priv_dir, Config),
    application:load(vonnegut),
    application:set_env(vonnegut, log_dirs, [filename:join(PrivDir, "data")]),
    application:set_env(vonnegut, segment_bytes, 177),
    application:set_env(vonnegut, index_max_bytes, 50),
    application:set_env(vonnegut, index_interval_bytes, 24),
    application:set_env(vonnegut, chain, [{discovery, local}]),
    application:ensure_all_started(vonnegut),
    crypto:start(),
    Config;
init_per_testcase(_, Config) ->
    PrivDir = ?config(priv_dir, Config),
    application:load(vonnegut),
    application:set_env(vonnegut, log_dirs, [filename:join(PrivDir, "data")]),
    application:set_env(vonnegut, segment_bytes, 86),
    application:set_env(vonnegut, index_max_bytes, 18),
    application:set_env(vonnegut, index_interval_bytes, 24),
    application:set_env(vonnegut, chain, [{discovery, local}]),
    application:ensure_all_started(vonnegut),
    crypto:start(),
    Config.

end_per_testcase(_, Config) ->
    application:stop(vonnegut),
    %% if we don't unload the settings will stick around in other suites
    application:unload(vonnegut),
    Config.

record_set_larger_than_max_segment(_Config) ->
    Topic = vg_test_utils:create_random_name(<<"log_roll_test_topic">>),
    Partition = 0,
    TopicPartitionDir = vg_utils:topic_dir(Topic, Partition),
    vg:create_topic(Topic),
    ?assert(filelib:is_dir(TopicPartitionDir)),

    RandomRecords = [#{record => M}
                      || M <- [crypto:strong_rand_bytes(60), crypto:strong_rand_bytes(60),
                               crypto:strong_rand_bytes(6), crypto:strong_rand_bytes(6),
                               crypto:strong_rand_bytes(60)]],
    vg:write(Topic, 0, RandomRecords),

    %% Total size of a 60 byte record when written to log becomes 86 bytes
    %% Since index interval is 24 and 86 > 24, 1 index entry of 6 bytes should exist for each as well
    ?assertEqual(filelib:file_size(filename:join([TopicPartitionDir, "00000000000000000000.index"])), 8),
    ?assertEqual(filelib:file_size(filename:join([TopicPartitionDir, "00000000000000000000.log"])), 86),
    ?assertEqual(filelib:file_size(filename:join([TopicPartitionDir, "00000000000000000001.index"])), 8),
    ?assertEqual(filelib:file_size(filename:join([TopicPartitionDir, "00000000000000000001.log"])), 86),

    %% Next 2 records create a log with 2 records of 6 bytes each (with headers they are 32 bytes)
    %% with ids 2 and 3. The third record (id 4) then goes in a new index and log
    ?assertEqual(filelib:file_size(filename:join([TopicPartitionDir, "00000000000000000002.index"])), 16),
    ?assertEqual(filelib:file_size(filename:join([TopicPartitionDir, "00000000000000000002.log"])), 64),
    ?assertEqual(filelib:file_size(filename:join([TopicPartitionDir, "00000000000000000004.index"])), 8),
    ?assertEqual(filelib:file_size(filename:join([TopicPartitionDir, "00000000000000000004.log"])), 86),

    %% regression test. check that a cold node (no data loaded) finds the right hwm for a topic

    application:stop(vonnegut),
    application:ensure_all_started(vonnegut),

    ?assertEqual(4, vg_topics:lookup_hwm(Topic, Partition)).

regenerate_index_test(_Config) ->
    Topic = vg_test_utils:create_random_name(<<"index_regen_test_topic">>),
    Partition = 0,
    TopicDir = vg_utils:topic_dir(Topic, Partition),
    vg:create_topic(Topic),

    vg:write(Topic, 0, [#{record => iolist_to_binary(lists:duplicate(rand:uniform(5), <<"A">>))}
                        || _ <- lists:seq(1, 50)]),

    AllFiles = filelib:wildcard(filename:join(TopicDir, "*.index")),
    SHAs = [begin
                {ok, B} = file:read_file(File),
                B
            end
            || File <- AllFiles],

    vg:regenerate_topic_index(Topic),

    AllFiles1 = filelib:wildcard(filename:join(TopicDir, "*.index")),
    SHAs1 = [begin
                 {ok, B} = file:read_file(File),
                 B
             end
             || File <- AllFiles1],
    ?assertMatch({ok,#{high_water_mark := 49,
                       partition := 0,
                       record_set :=
                           [#{id := 45}]}},
                 vg:fetch(Topic, 0, 45, 1)),

    ?assertEqual(SHAs, SHAs1),
    ok.

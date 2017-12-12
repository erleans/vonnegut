-module(cleanup_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

-include("vg.hrl").

all() ->
    [delete_policy].

init_per_testcase(_, Config) ->
    PrivDir = ?config(priv_dir, Config),
    application:load(vonnegut),
    application:set_env(vonnegut, log_dirs, [filename:join(PrivDir, "data")]),
    application:set_env(vonnegut, segment_bytes, 86),
    application:set_env(vonnegut, index_max_bytes, 18),
    application:set_env(vonnegut, log_cleaner, true),
    application:set_env(vonnegut, index_interval_bytes, 24),
    application:set_env(vonnegut, log_retention_minutes, 5),
    application:set_env(vonnegut, chain, [{discovery, local}]),
    application:ensure_all_started(vonnegut),
    crypto:start(),
    Config.

end_per_testcase(_, Config) ->
    application:stop(vonnegut),
    application:unload(vonnegut),
    Config.

delete_policy(_Config) ->
    {ok, LogRetentionMinutes} = application:get_env(vonnegut, log_retention_minutes),
    Topic = vg_test_utils:create_random_name(<<"test_topic">>),
    Partition = 0,
    TopicPartitionDir = vg_utils:topic_dir(Topic, Partition),
    vg:create_topic(Topic),
    ?assert(filelib:is_dir(TopicPartitionDir)),

    RandomRecords = [#{record => M}
                      || M <- [crypto:strong_rand_bytes(60), crypto:strong_rand_bytes(60)]],
    vg:write(Topic, Partition, RandomRecords),

    %% Verify 2 segments have been created
    Segment0 = filename:join([TopicPartitionDir, "00000000000000000000.log"]),
    Segment1 = filename:join([TopicPartitionDir, "00000000000000000001.log"]),
    ?assert(filelib:is_regular(Segment0)),
    ?assert(filelib:is_regular(Segment1)),

    meck:new(filelib, [unstick, passthrough]),
    %% Mock last_modified return for Segment0 to be >= LogRetentionMinutes so it is deleted
    Now = calendar:local_time(),
    meck:expect(filelib, last_modified, fun(Segment) when Segment =:= Segment0 ->
                                                dec_datetime_by_mins(Now, LogRetentionMinutes+1);
                                           (Segment) ->
                                                meck:passthrough([Segment])
                                        end),
    %% Execute the cleaner
    vg_cleaner:run_cleaner(Topic, 0),
    meck:unload(filelib),

    %% Verify Segment0 has been deleted but not Segment1
    ?assertEqual(filelib:is_regular(Segment0), false),
    ?assertEqual(filelib:is_regular(Segment1), true).

%%

dec_datetime_by_mins(DateTime, Minutes) ->
    Seconds = calendar:datetime_to_gregorian_seconds(DateTime),
    Seconds1 = Seconds - (Minutes * 60),
    calendar:gregorian_seconds_to_datetime(Seconds1).

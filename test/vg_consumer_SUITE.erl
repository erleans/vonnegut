-module(vg_consumer_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() ->
    [from_zero, multi_topic_fetch].

init_per_testcase(_, Config) ->
    PrivDir = ?config(priv_dir, Config),
    application:load(vonnegut),
    application:set_env(vonnegut, log_dirs, [filename:join(PrivDir, "data")]),
    application:set_env(vonnegut, segment_bytes, 86),
    application:set_env(vonnegut, index_max_bytes, 18),
    application:set_env(vonnegut, index_interval_bytes, 24),
    application:set_env(vonnegut, chain, [{discovery, local}]),
    application:start(shackle),
    application:ensure_all_started(vonnegut),
    crypto:start(),
    Config.

end_per_testcase(_, Config) ->
    application:unload(vonnegut),
    Config.

from_zero(_Config) ->
    Topic = vg_test_utils:create_random_name(<<"consumer_SUITE_test_topic">>),
    Partition = 0,
    TopicPartitionDir = vg_utils:topic_dir(Topic, Partition),
    ok = vg:create_topic(Topic),
    ?assert(filelib:is_dir(TopicPartitionDir)),

    %% make sure there's enough time for the
    %% listeners to come up
    timer:sleep(250),

    ok = vg_client_pool:start(),
    ?assertMatch({ok, 0},
                 vg_client:produce(Topic, [{<<"key">>, <<"record 1 wasn't long enough to make wrapping fail">>},
                                           <<"record 2">>])),

    {ok, #{Topic := #{0 := #{record_set := Data, high_water_mark := HWM}}}} = vg_client:fetch(Topic, 0),
    ?assertEqual(1, HWM),
    ?assertMatch([#{id := 0, record := {<<"key">>, <<"record 1 wasn't long enough to make wrapping fail">>}}], Data),
    {ok, #{Topic := #{0 := #{record_set := Data1, high_water_mark := HWM1}}}} = vg_client:fetch(Topic, 1),
    ?assertEqual(1, HWM1),
    ?assertMatch([#{id := 1, record := <<"record 2">>}], Data1),
    vg_client_pool:stop(),
    ok.

multi_topic_fetch(_Config) ->
    Topic1 = vg_test_utils:create_random_name(<<"consumer_SUITE_test_topic-1">>),
    Topic2 = vg_test_utils:create_random_name(<<"consumer_SUITE_test_topic-2">>),
    
    ok = vg:create_topic(Topic1),
    ok = vg:create_topic(Topic2),

    %% make sure there's enough time for the
    %% listeners to come up
    timer:sleep(250),

    ok = vg_client_pool:start(),
    ?assertMatch({ok, 0},
                 vg_client:produce(Topic1, [{<<"key">>, <<"topic 1 record 1">>},
                                            <<"topic 1 record 2">>])),

    ?assertMatch({ok, 0},
                 vg_client:produce(Topic2, [{<<"key-2">>, <<"topic 2 record 1">>},
                                            <<"topic 2 record 2">>])),

    {ok, #{Topic1 := #{0 := #{record_set := Data, high_water_mark := HWM}},
           Topic2 := #{0 := #{record_set := Data2, high_water_mark := HWM2}}}} = vg_client:fetch([{Topic1, 0},
                                                                                                  {Topic2, 1}]),

    ?assertEqual(1, HWM),
    ?assertMatch([#{id := 0, record := {<<"key">>, <<"topic 1 record 1">>}}], Data),

    ?assertEqual(1, HWM2),
    ?assertMatch([#{id := 1, record := <<"topic 2 record 2">>}], Data2),

    vg_client_pool:stop(),
    ok.

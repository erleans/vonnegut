-module(vg_consumer_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

-include("test_utils.hrl").

all() ->
    [from_zero, multi_topic_fetch, fetch_unknown, fetch_higher_than_hwm, regression_2_23_18].

init_per_suite(Config) ->
    PrivDir = ?config(priv_dir, Config),
    application:load(vonnegut),
    application:set_env(vonnegut, client_pool_size, 2),
    application:set_env(vonnegut, log_dirs, [filename:join(PrivDir, "data")]),
    application:set_env(vonnegut, segment_bytes, 86),
    application:set_env(vonnegut, index_max_bytes, 18),
    application:set_env(vonnegut, index_interval_bytes, 24),
    application:set_env(vonnegut, client, [{endpoints, [{"127.0.0.1", 5555}]}]),
    application:set_env(vonnegut, chain, [{discovery, local}]),
    application:start(shackle),
    application:ensure_all_started(vonnegut),
    crypto:start(),
    Config.

end_per_suite(Config) ->
    application:stop(vonnegut),
    application:unload(vonnegut),
    Config.

init_per_testcase(_, Config) ->
    ok = vg_client_pool:start(#{reconnect => false}),
    Config.

end_per_testcase(_, _Config) ->
    vg_client_pool:stop(),
    ok.

from_zero(_Config) ->
    Topic = vg_test_utils:create_random_name(<<"consumer_SUITE_test_topic">>),
    {ok, _} = vg_client:ensure_topic(Topic),
    Partition = 0,
    TopicPartitionDir = vg_utils:topic_dir(Topic, Partition),
    ?assert(filelib:is_dir(TopicPartitionDir)),

    %% make sure there's enough time for the
    %% listeners to come up
    timer:sleep(250),

    ?assertMatch({ok, 0},
                 vg_client:produce(Topic, [#{key => <<"key">>,
                                             value => <<"record 1 wasn't long enough to make wrapping fail">>}])),
    ?assertMatch({ok, 1},
                 vg_client:produce(Topic, [<<"record 2">>])),
    {ok, #{Topic := #{0 := #{record_batches := Data, high_water_mark := HWM}}}} = vg_client:fetch(Topic, 0),
    ?assertEqual(1, HWM),
    ?assertMatch([#{offset := 0, key := <<"key">>, value := <<"record 1 wasn't long enough to make wrapping fail">>}], Data),
    {ok, #{Topic := #{0 := #{record_batches := Data1, high_water_mark := HWM1}}}} = vg_client:fetch(Topic, 1),
    ?assertEqual(1, HWM1),
    ?assertMatch([#{offset := 1, value := <<"record 2">>}], Data1),

    ok.

multi_topic_fetch(_Config) ->

    Topic1 = vg_test_utils:create_random_name(<<"consumer_SUITE_test_topic-1">>),
    Topic2 = vg_test_utils:create_random_name(<<"consumer_SUITE_test_topic-2">>),

    ok = vg:create_topic(Topic1),
    ok = vg:create_topic(Topic2),

    %% make sure there's enough time for the
    %% listeners to come up
    timer:sleep(250),

    ?assertMatch({ok, 0},
                 vg_client:produce(Topic1, [#{timestamp => erlang:system_time(millisecond),
                                              key => <<"key">>, value => <<"topic 1 record 1">>}])),
    ?assertMatch({ok, 1},
                 vg_client:produce(Topic1, [<<"topic 1 record 2">>])),

    ?assertMatch({ok, 0},
                 vg_client:produce(Topic2, [#{timestamp => erlang:system_time(millisecond),
                                              key => <<"key-2">>, value => <<"topic 2 record 1">>}])),
    ?assertMatch({ok, 1},
                 vg_client:produce(Topic2, [<<"topic 2 record 2">>])),

    {ok, #{Topic1 := #{0 := #{record_batches := Data, high_water_mark := HWM}},
           Topic2 := #{0 := #{record_batches := Data2, high_water_mark := HWM2}}}} = vg_client:fetch([{Topic1, 0, #{}},
                                                                                                  {Topic2, 1, #{}}]),

    ?assertEqual(1, HWM),
    ?assertMatch([#{offset := 0, key := <<"key">>, value := <<"topic 1 record 1">>}], Data),

    ?assertEqual(1, HWM2),
    ?assertMatch([#{offset := 1, value := <<"topic 2 record 2">>}], Data2),

    ok.

fetch_unknown(_Config) ->
    Topic = vg_test_utils:create_random_name(<<"consumer_SUITE_test_topic">>),

    %% make sure there's enough time for the
    %% listeners to come up
    timer:sleep(250),

    ?assertMatch({error, {Topic, not_found}}, vg_client:fetch(Topic, 0)),

    ok.

fetch_higher_than_hwm(_Config) ->
    Topic = vg_test_utils:create_random_name(<<"consumer_SUITE_fetch_higher_than_hwm">>),
    {ok, _} = vg_client:ensure_topic(Topic),
    Partition = 0,
    TopicPartitionDir = vg_utils:topic_dir(Topic, Partition),
    ?assert(filelib:is_dir(TopicPartitionDir)),

    %% fetch from an empty log
    {ok, #{Topic := #{0 := #{record_batches := Data0, high_water_mark := HWM0}}}} = vg_client:fetch(Topic, 1),
    ?assertEqual(-1, HWM0),
    ?assertMatch([], Data0),

    %% fetch with a limit from an empty log
    {ok, #{Topic := #{0 := #{record_batches := Data0, high_water_mark := HWM0}}}} = vg_client:fetch(Topic, 1, 1),
    ?assertEqual(-1, HWM0),
    ?assertMatch([], Data0),

    %% make sure there's enough time for the
    %% listeners to come up
    timer:sleep(250),

    ?assertMatch({ok, 0},
                 vg_client:produce(Topic, [#{key => <<"some key">>,
                                             value => <<"rsome value">>}])),
    {ok, #{Topic := #{0 := #{record_batches := Data, high_water_mark := HWM1}}}} = vg_client:fetch(Topic, 1),
    ?assertEqual(0, HWM1),
    ?assertMatch([], Data),

    ok.

%% fetch from 5 with limit 1000 was timing out.
%% issue was vonnegut claiming to send more data than it actually would, leaving the client expecting more
regression_2_23_18(_Config) ->
    Topic = vg_test_utils:create_random_name(<<"consumer_SUITE_regression_2-23-18">>),
    {ok, _} = vg_client:ensure_topic(Topic),
    Partition = 0,
    TopicPartitionDir = vg_utils:topic_dir(Topic, Partition),
    ?assert(filelib:is_dir(TopicPartitionDir)),

    %% make sure there's enough time for the
    %% listeners to come up
    timer:sleep(250),

    vg_client:produce(Topic, [#{value => <<"some value">>}]),
    {ok, #{Topic := #{0 := #{record_batches := _Data, high_water_mark := HWM1}}}} = vg_client:fetch(Topic, 5, 1000),
    ?assertEqual(0, HWM1),

    ok.

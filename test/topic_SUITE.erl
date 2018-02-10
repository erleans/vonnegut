-module(topic_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() ->
    [creation, write_empty, write, index_bug, limit, index_limit,
     startup_index_correctness, many, verify_lazy_load,
     local_client_test, last_in_index].

init_per_suite(Config) ->
    PrivDir = ?config(priv_dir, Config),
    %% clear env from other suites
    application:unload(vonnegut),
    application:load(vonnegut),
    application:load(partisan),
    application:set_env(partisan, partisan_peer_service_manager, partisan_default_peer_service_manager),
    application:set_env(vonnegut, log_dirs, [filename:join(PrivDir, "data")]),
    application:set_env(vonnegut, chain, [{discovery, local}]),
    application:set_env(vonnegut, client, [{endpoints, [{"127.0.0.1", 5555}]}]),
    application:set_env(vonnegut, client_pool_size, 2),
    {ok, _} = application:ensure_all_started(vonnegut),
    Config.

end_per_suite(_Config) ->
    application:stop(vonnegut),
    application:unload(vonnegut),
    ok.

init_per_testcase(_, Config) ->
    ok = vg_client_pool:start(#{reconnect => false}),
    Topic = vg_test_utils:create_random_name(<<"topic_SUITE_default_topic">>),
    {ok, _} = vg_client:ensure_topic(Topic),
    [{topic, Topic} | Config].

end_per_testcase(_, _Config) ->
    vg_client_pool:stop(),
    ok.

creation(_Config) ->
    Topic = vg_test_utils:create_random_name(<<"creation_test_topic">>),
    Partition = 0,
    TopicPartitionDir = vg_utils:topic_dir(Topic, Partition),
    vg:create_topic(Topic),
    ?assert(filelib:is_dir(TopicPartitionDir)).

%% leaving this in as it occasionally hits a quasi race, so if we
%% start hitting intermittent failures here, we might have a regression
write_empty(_Config) ->
    Topic = vg_test_utils:create_random_name(<<"topic_SUITE_default_topic">>),
    {ok, _} = vg_client:ensure_topic(Topic),
    spawn(fun() -> vg_client:produce(Topic, <<"fleerp">>) end),
    {ok, #{Topic := #{0 := #{record_batches := Reply}}}} = vg_client:fetch(Topic, 0),
    case Reply of
        [#{value := <<"fleerp">>}] -> % write then read
            ok;
        [] -> % read then write
            ok;
        _ ->
            ct:pal("got ~p", [Reply]),
            error(bad_return)
    end.

write(Config) ->
    Topic = ?config(topic, Config),
    Anarchist = <<"no gods no masters">>,
    [begin
         {ok, R} = vg_client:produce(Topic, Anarchist),
         ct:pal("reply: ~p", [R])
     end
     || _ <- lists:seq(1, rand:uniform(20))],
    Communist =  <<"from each according to their abilities, to "
                   "each according to their needs">>,
    {ok, R1} = vg_client:produce(Topic, Communist),
    ct:pal("reply: ~p", [R1]),
    {ok, #{Topic := #{0 := #{record_batches := Reply}}}} = vg_client:fetch(Topic, R1),
    ?assertMatch([#{value := Communist}], Reply),

    {ok, #{Topic := #{0 := #{record_batches := Reply1}}}} = vg_client:fetch(Topic, R1 - 1),
    ?assertMatch([#{value := Anarchist}, #{value := Communist}], Reply1).

index_bug(Config) ->
    Topic = ?config(topic, Config),

    %% write enough data to cause index creation but not two entries
    {ok, _}  = vg_client:produce(Topic,
                                 lists:duplicate(100, <<"123456789abcdef">>)),

    %% fetch from 0 to make sure that they're all there
    {ok, #{Topic := #{0 := #{record_batches := Reply}}}} = vg_client:fetch(Topic, 0),
    ?assertEqual(100, length(Reply)),

    %% now query for something before the first index marker
    {ok, #{Topic := #{0 := #{record_batches := Reply2,
                             high_water_mark := HWM}}}} =
        vg_client:fetch(Topic, 10),

    ?assertEqual(99, HWM),

    %% this is a passing version before the bugfix
    %% ?assertEqual([], Reply2).

    %% ?assertEqual(90, length(Reply2)),
    %% change with 0.11.0 RecorBatch storage
    ?assertEqual(100, length(Reply2)),

    %% write enough more data for another entry to hit the second clause
    {ok, _}  = vg_client:produce(Topic,
                                 lists:duplicate(100, <<"123456789abcdef">>)),

    {ok, #{Topic := #{0 := #{record_batches := Reply3}}}} = vg_client:fetch(Topic, 0),
    ?assertEqual(200, length(Reply3)),

    {ok, #{Topic := #{0 := #{record_batches := Reply4,
           high_water_mark := HWM4}}}} = vg_client:fetch(Topic, 10),

    ?assertEqual(199, HWM4),
    ?assertEqual(200, length(Reply4)).

last_in_index(Config) ->
    Topic = ?config(topic, Config),

    [{ok, _}  = vg_client:produce(Topic,
                                  lists:duplicate(100, <<"123456789abcdef">>))
     || _ <- lists:seq(1, 100)],

    %% try to force flush
    application:stop(vonnegut),

    {ok, TopicDir0} = application:get_env(vonnegut, log_dirs),
    TopicDir = TopicDir0 ++ "/" ++  binary_to_list(Topic) ++ "-0/",
    Filename = vg_utils:index_file(TopicDir, 0),
    ct:pal("topic dir ~p, filename ~p", [TopicDir, Filename]),
    ?assertNotEqual({0, 0}, vg_log_segments:last_in_index(TopicDir, Filename, 1)).


limit(Config) ->
    Topic = ?config(topic, Config),

    {ok, P} = vg_client:produce(Topic,
                                 lists:duplicate(100, <<"123456789abcdef">>)),
    ?assertEqual(99, P),
    {ok, #{Topic := #{0 := #{record_batches := Reply}}}} = vg_client:fetch(Topic),
    ?assertEqual(100, length(Reply)),

    {ok, #{Topic := #{0 := #{record_batches := Reply2}}}} =
               vg_client:fetch([{Topic, 0, #{max_bytes => 1000}}]),
    %% ?assertEqual(24, length(Reply2)),
    %% how is it we are geting more with the new format...
    ?assertEqual(37, length(Reply2)),

    {ok, #{Topic := #{0 := #{record_batches := []}}}} =
               vg_client:fetch([{Topic, 0, #{max_bytes => 1}}]),

    ok.

index_limit(Config) ->
    Topic = ?config(topic, Config),

    [{ok, _} = vg_client:produce(Topic,
                                <<"123456789abcdef">>) || _ <- lists:seq(1, 100)],

    {ok, #{Topic := #{0 := #{record_batches := Reply}}}} = vg_client:fetch(Topic, 0),
    ?assertEqual(100, length(Reply)),

    {ok, #{Topic := #{0 := #{record_batches := Reply2}}}} = vg_client:fetch(Topic, 0, 50),
    ?assertEqual(50, length(Reply2)),
    %% ?assertEqual(50, length(Reply2)),

    %% max_bytes overrides max_index
    {ok, #{Topic := #{0 := #{record_batches := Reply3}}}} = vg_client:fetch([{Topic, 0, #{limit => 50, max_bytes => 1000}}]),
    ?assertEqual(12, length(Reply3)),
    %% ?assertEqual(24, length(Reply3)),

    %% limit returns Offset to Offset+Limit
    {ok, #{Topic := #{0 := #{record_batches := Reply4}}}} = vg_client:fetch([{Topic, 10, #{limit => 20}}]),
    ?assertEqual(20, length(Reply4)),
    ?assertMatch(#{offset := 10}, hd(Reply4)),
    ?assertMatch(#{offset := 29}, hd(lists:reverse(Reply4))),

    %% -1 Offset returns HWM-Limit to HWM
    {ok, #{Topic := #{0 := #{record_batches := Reply5}}}} = vg_client:fetch([{Topic, -1, #{limit => 20}}]),
    ?assertMatch(#{offset := 80}, hd(Reply5)),
    ?assertMatch(#{offset := 99}, hd(lists:reverse(Reply5))),
    ?assertEqual(20, length(Reply5)),

    %% -1 Offset with limit larger than HWM starts from 0
    {ok, #{Topic := #{0 := #{record_batches := Reply6}}}} = vg_client:fetch([{Topic, -1, #{limit => 200}}]),
    ?assertEqual(100, length(Reply6)),

    {ok, #{Topic := #{0 := #{record_batches := []}}}} = vg_client:fetch([{Topic, 0, #{max_bytes => 1}}]),

    ok.


many(Config) ->
    TopicCount = 1000,
    TimeLimit = 100000,

    Start = erlang:monotonic_time(milli_seconds),
    [begin
         N = integer_to_binary(N0),
         Topic = vg_test_utils:create_random_name(<<"many-topic-", N/binary>>),
         %% adding a record to the topic will create it under current settings
         ct:pal("adding to topic: ~p", [Topic]),
         {ok, _} = vg_client:ensure_topic(Topic),
         {ok, _} = vg_client:produce(Topic, [<<"woo">>])
     end || N0 <- lists:seq(1, TopicCount)],
    Duration = erlang:monotonic_time(milli_seconds) - Start,
    ct:pal("creating ~p topics took ~p ms", [TopicCount, Duration]),
    ?assert(Duration < TimeLimit),
    Config.

wait_for_start(Topic) ->
    wait_for_start(Topic, 5000).

wait_for_start(_Topic, 0) ->
    error(waited_too_long);
wait_for_start(Topic, N) ->
    case vg_client:fetch(Topic, 0, 1) of
        {ok, _} = _OK ->
            %%ct:pal("ok ~p", [_OK]),
            timer:sleep(150),
            ok;
        {error, no_socket} ->
            timer:sleep(1),
            wait_for_start(Topic, N - 1)
    end.

startup_index_correctness(Config) ->
    %% we actually want the reconnect behavior here
    ok = vg_client_pool:stop(),
    ok = vg_client_pool:start(#{reconnect => true}),

    Topic = ?config(topic, Config),
    ct:pal("STARTING TEST"),

    {ok, _} = vg_client:produce(Topic,
                                lists:duplicate(1, <<"123456789abcdef000000000">>)),
    {ok, _} = vg_client:produce(Topic,
                                lists:duplicate(1, <<"123456789abcdef111111111">>)),


    [begin
         application:stop(vonnegut),
         %% bleh circle
         timer:sleep(750),
         {ok, _} = application:ensure_all_started(vonnegut),
         wait_for_start(Topic),
         A = integer_to_binary(N),
         B = integer_to_binary(N + 1),
         C = integer_to_binary(N + 2),
         M = N + 1,
         {ok, Q} = vg_client:produce(Topic, [<<"123456789abcdef-", A/binary>>,
                                             <<"123456789abcdef-", B/binary>>]),
         ?assertEqual(M, Q),
         Y = M + 1,
         {ok, Y} = vg_client:produce(Topic, <<"123456789abcdef-", C/binary>>)
     end
     || N <- lists:seq(2, 11, 3)],

    %% -1 Offset returns HWM-Limit to HWM
    {ok, #{Topic := #{0 := #{record_batches := ReplyN}}}} = vg_client:fetch([{Topic, 0, #{limit => 2000}}]),
    {ok, #{Topic := #{0 := #{record_batches := Reply0}}}} = vg_client:fetch([{Topic, -1, #{limit => 2}}]),
    ct:pal("reply 0 ~p", [Reply0]),
    ct:pal("whole set ~p", [ReplyN]),

    %% 11 and 12 are in one RecordBatch
    ?assertEqual(3, length(Reply0)),
    ?assertMatch(#{offset := 12}, hd(Reply0)),
    ?assertMatch(#{offset := 13}, hd(lists:reverse(Reply0))),

    {ok, #{Topic := #{0 := #{record_batches := Reply1}}}} = vg_client:fetch([{Topic, 0, #{limit => 100}}]),
    ?assertEqual(14, length(Reply1)),
    ?assertMatch(#{offset := 13}, hd(lists:reverse(Reply1))),
    ok.

%% verify the active topic segment process is not started until needed
verify_lazy_load(_Config) ->
    Topic = vg_test_utils:create_random_name(<<"verify_lazy_load">>),
    Partition = 0,
    TopicPartitionDir = vg_utils:topic_dir(Topic, Partition),
    vg:create_topic(Topic),
    ?assert(filelib:is_dir(TopicPartitionDir)),

    {ok, _}  = vg_client:produce(Topic,
                                 lists:duplicate(100, <<"123456789abcdef">>)),

    %% fetch from 0 to make sure that they're all there
    {ok, #{Topic := #{0 := #{record_batches := Reply}}}} = vg_client:fetch(Topic, 0),
    ?assertEqual(100, length(Reply)),

    application:stop(vonnegut),

    %% delay on getting the elli port back can cause restarting to fail so pause for a bit
    timer:sleep(500),

    {ok, _} = application:ensure_all_started(vonnegut),
    wait_for_start(Topic),

    ?assertEqual(undefined, gproc:whereis_name({n,l,{active, Topic, Partition}})),

    {ok, #{Topic := #{0 := #{record_batches := Reply2}}}} = vg_client:fetch(Topic, 0),
    ?assertEqual(100, length(Reply2)),

    ?assertEqual(undefined, gproc:whereis_name({n,l,{active, Topic, Partition}})),

    %% writing starts the process
    {ok, _}  = vg_client:produce(Topic,
                                 lists:duplicate(100, <<"123456789abcdef">>)),

    ?assertNotEqual(undefined, gproc:whereis_name({n,l,{active, Topic, Partition}})).

local_client_test(Config) ->
    Topic = ?config(topic, Config),
    vg:write(Topic, 0, <<"foo">>),
    {ok, Ret} = vg:fetch(Topic),
    ?assertMatch(#{high_water_mark := 0,
                   partition := 0,
                   record_batches :=
                       [#{offset := 0,
                          value := <<"foo">>}]},
                 Ret),
    ok.

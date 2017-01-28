-module(topic_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() ->
    [creation, write, ack].

init_per_suite(Config) ->
    PrivDir = ?config(priv_dir, Config),
    lager:start(),
    application:load(vonnegut),
    application:set_env(vonnegut, log_dirs, [filename:join(PrivDir, "data")]),
    {ok, _} = application:ensure_all_started(vonnegut),
    Config.

end_per_suite(Config) ->
    application:stop(vonnegut),
    Config.

init_per_testcase(_, Config) ->
    ok = vg_client_pool:start(),
    Topic = vg_test_utils:create_random_name(<<"topic_SUITE_default_topic">>),
    vg:create_topic(Topic),
    [{topic, Topic} | Config].

end_per_testcase(_, Config) ->
    vg_client_pool:stop(),
    Config.


creation(_Config) ->
    Topic = vg_test_utils:create_random_name(<<"creation_test_topic">>),
    Partition = 0,
    TopicPartitionDir = vg_utils:topic_dir(Topic, Partition),
    vg:create_topic(Topic),
    ?assert(filelib:is_dir(TopicPartitionDir)).

write(Config) ->
    Topic = ?config(topic, Config),
    Anarchist = <<"no gods no masters">>,
    [begin
         R = vg_client:produce(Topic, Anarchist),
         ct:pal("reply: ~p", [R])
     end
     || _ <- lists:seq(1, rand:uniform(20))],
    Communist =  <<"from each according to their abilities, to "
                   "each according to their needs">>,
    #{topic := Topic, offset := R1} = vg_client:produce(Topic, Communist),
    ct:pal("reply: ~p", [R1]),
    #{partitions := [#{message_set := Reply}]} = vg_client:fetch(Topic, R1),
    ?assertMatch([#{record := Communist}], Reply),
    #{partitions := [#{message_set := Reply1}]} = vg_client:fetch_until(Topic, R1 - 1, R1),
    ?assertMatch([#{record := Anarchist}, #{record := Communist}], Reply1).

ack(Config) ->
    Topic = ?config(topic, Config),

    %% write some random messages to the topic
    Msgs = [begin
                Msg = crypto:strong_rand_bytes(60),
                {ok, _Id} = vg:write(Topic, Msg),
                Msg
            end
           || _ <- lists:seq(1, rand:uniform(20))],

    %% verify written
    #{message_set := Reply} = vg:fetch(Topic),
    Records = [Record || #{record := Record} <- Reply],
    ?assertEqual(Msgs, Records),

    %% verify history
    HistoryTab = binary_to_atom(<<Topic/binary, 0>>, utf8),
    History = ets:tab2list(HistoryTab),
    {_, HistoryMsgs} = lists:unzip(lists:sort(History)),
    Header = vg_protocol:encode_fetch_response(Topic, 0, 0, length(Msgs), iolist_size(HistoryMsgs)),
    #{partitions :=
          [#{high_water_mark := HighWaterMark, % maybe not a good test
             message_set := HistorySet}]} =
        vg_protocol:decode_fetch_response(iolist_to_binary([Header, HistoryMsgs])),
    ?assertEqual(Msgs, [Record || #{record := Record} <- HistorySet]),

    %% Ack to the latest Id
    vg_active_segment:ack(Topic, 0, HighWaterMark),
    %% History should be empty now
    ?assertEqual([], ets:tab2list(HistoryTab)).

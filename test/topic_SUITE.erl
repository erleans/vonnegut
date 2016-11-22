-module(topic_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() ->
    [creation, write].

init_per_suite(Config) ->
    PrivDir = ?config(priv_dir, Config),
    lager:start(),
    application:load(vonnegut),
    application:set_env(vonnegut, log_dirs, [filename:join(PrivDir, "data")]),
    application:ensure_all_started(vonnegut),
    ok = vg_client_pool:start(),
    Topic = vg_test_utils:create_random_name(<<"default_topic">>),
    vg:create_topic(Topic),
    [{topics, [Topic]} | Config].

end_per_suite(Config) ->
    vg_client_pool:stop(),
    application:stop(vonnegut),
    Config.

creation(Config) ->
    Topic = vg_test_utils:create_random_name(<<"test_topic">>),
    Partition = 0,
    TopicPartitionDir = vg_utils:topic_dir(Topic, Partition),
    vg:create_topic(Topic),
    ?assert(filelib:is_dir(TopicPartitionDir)),
    add_topic(Topic, Config).

write(Config) ->
    Topic = get_topic(Config),
    Anarchist = <<"no gods no masters">>,
    [begin
         [{Topic, R}] = vg_client:produce(Topic, Anarchist),
         ct:pal("reply: ~p", [R])
     end
     || _ <- lists:seq(1, rand:uniform(20))],
    Communist =  <<"from each according to their abilities, to "
                   "each according to their needs">>,
    [{Topic, [{0, 0, R1}]}] = vg_client:produce(Topic, Communist),
    ct:pal("reply: ~p", [R1]),
    timer:sleep(1000),
    [Reply] = vg_client:fetch(Topic, R1),
    ?assertEqual(Communist, Reply),
    Reply1 = vg_client:fetch(Topic, R1 - 1),
    ?assertEqual([Anarchist, Communist], Reply1),
    Config.

%%% helpers

add_topic(Topic, Config) ->
    {_, Topics} = lists:keyfind(topics, 1, Config),
    Topics1 = [Topic | Topics],
    lists:keyreplace(topics, 1, Config, {topics, Topics1}).

get_topic(Config) ->
    {_, Topics} = lists:keyfind(topics, 1, Config),
    lists:nth(rand:uniform(length(Topics)), Topics).

-module(kafka_client_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

-include_lib("brod/include/brod.hrl").

all() ->
    [get_metadata, produce].

init_per_suite(Config) ->
    PrivDir = ?config(priv_dir, Config),
    application:load(vonnegut),
    application:set_env(vonnegut, log_dirs, [filename:join(PrivDir, "data")]),
    application:set_env(vonnegut, segment_bytes, 86),
    application:set_env(vonnegut, index_max_bytes, 18),
    application:set_env(vonnegut, index_interval_bytes, 24),
    application:set_env(vonnegut, chain, [{discovery, local}]),
    crypto:start(),

    Port = 5555,
    Host = <<"127.0.0.1">>,
    Hosts = [{"127.0.0.1", Port}],

    application:ensure_all_started(vonnegut),
    application:ensure_all_started(brod),

    ok = brod:start_client(Hosts, brod_client_1, []),

    [{host, Host}, {port, Port}, {hosts, Hosts} | Config].

end_per_suite(Config) ->
    application:unload(vonnegut),
    Config.

get_metadata(Config) ->
    Host = ?config(host, Config),
    Port = ?config(port, Config),
    Hosts = ?config(hosts, Config),

    Topic = vg_test_utils:create_random_name(<<"kafka_get_metadata">>),
    ok = vg:create_topic(Topic),

    %% same host will be in broker list twice because we send the same broker as the tail
    {ok,
     [{brokers,
       [[{node_id,0},{host,Host},{port,Port}],
        [{node_id,0},{host,Host},{port,Port}]]},
      {topic_metadata, TMs}]} = brod:get_metadata(Hosts),
    ?assert(lists:any(fun(TM) -> lists:member({topic,Topic}, TM) end, TMs)).

produce(Config) ->
    Hosts = ?config(hosts, Config),
    Topic = vg_test_utils:create_random_name(<<"kafka_produce">>),
    ok = vg:create_topic(Topic),
    brod:start_producer(brod_client_1, Topic, []),

    Key = <<"I'm a key">>,
    M = <<"hello from brod">>,
    brod:produce_sync(brod_client_1,
                      Topic,
                      0,
                      Key,
                      M),

    ?assertMatch({ok, [#kafka_message{key=Key,
                                      value=M} | _]}, brod:fetch(Hosts, Topic, 0, 0)),


    ok.

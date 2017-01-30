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
    crypto:start(),

    Port = 5555,
    Host = <<"localhost">>,
    Hosts = [{"localhost", Port}],

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
    Partition = 0,
    ok = vg:create_topic(Topic),

    {ok, {kpro_MetadataResponse,
          [{kpro_Broker,0,Host,Port}],
          [{kpro_TopicMetadata,no_error,Topic,
            [{kpro_PartitionMetadata,no_error,Partition,0,[],[]}]}| _]}} = brod:get_metadata(Hosts).

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

-module(kafka_client_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() ->
    [get_metadata].

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
    application:unload(vonnegut),
    Config.

get_metadata(_Config) ->
    Topic = vg_test_utils:create_random_name(<<"kafka_get_metadata">>),
    Partition = 0,
    ok = vg:create_topic(Topic),

    Port = 5555,
    Host = <<"localhost">>,
    Hosts = [{"localhost", Port}],

    {ok, {kpro_MetadataResponse,
          [{kpro_Broker,0,Host,Port}],
          [{kpro_TopicMetadata,no_error,Topic,
            [{kpro_PartitionMetadata,no_error,Partition,0,[],[]}]}| _]}} = brod:get_metadata(Hosts).

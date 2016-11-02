-module(vg_consumer_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() ->
    [from_zero].

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
    Config.

from_zero(_Config) ->
    %% Topic = vg_test_utils:create_random_name(<<"test_topic">>),
    Topic = <<"test_topic">>,
    Partition = 0,
    TopicPartitionDir = vg_utils:topic_dir(Topic, Partition),
    ok = vg:create_topic(Topic),
    ?assert(filelib:is_dir(TopicPartitionDir)),

    vg:write(Topic, [<<"message 1">>, <<"message 2">>]),

    SocketOpts = [binary,
                  {buffer, 65535},
                  {nodelay, true},
                  {packet, raw},
                  {send_timeout, 5000},
                  {send_timeout_close, true}],
    shackle_pool:start(vg_client_pool, vg_client, [{ip, "127.0.0.1"},
                                                   {port, 5555},
                                                   {reconnect, true},
                                                   {reconnect_time_max, 120000},
                                                   {reconnect_time_min, none},
                                                   {socket_options, SocketOpts}], [{backlog_size, 1024},
                                                                                   {pool_size, 2},
                                                                                   {pool_strategy, random}]),
    Data = shackle:call(vg_client_pool, {fetch, <<"test_topic">>, 0}),
    ?assertEqual([<<"message 2">>, <<"message 1">>], Data),
    ok.

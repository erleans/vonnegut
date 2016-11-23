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
    Topic = vg_test_utils:create_random_name(<<"test_topic">>),
    Partition = 0,
    TopicPartitionDir = vg_utils:topic_dir(Topic, Partition),
    ok = vg:create_topic(Topic),
    ?assert(filelib:is_dir(TopicPartitionDir)),

    ok = vg_client_pool:start(),
    ?assertMatch(#{topic := Topic, offset := 0},
                 vg_client:produce(Topic, [<<"message 1 wasn't long enough to make wrapping fail">>,
                                           <<"message 2">>])),

    #{message_set := Data} = vg_client:fetch(Topic),
    ?assertEqual([<<"message 1 wasn't long enough to make wrapping fail">>, <<"message 2">>], Data),
    vg_client_pool:stop(),
    ok.

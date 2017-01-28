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
    ?assertMatch(#{topic := Topic, offset := 0},
                 vg_client:produce(Topic, [<<"record 1 wasn't long enough to make wrapping fail">>,
                                           <<"record 2">>])),

    #{partitions := [#{record_set := Data}]} = vg_client:fetch_until(Topic, 1),
    ?assertMatch([#{id := 0, record := <<"record 1 wasn't long enough to make wrapping fail">>},
                  #{id := 1, record := <<"record 2">>}], Data),
    vg_client_pool:stop(),
    ok.

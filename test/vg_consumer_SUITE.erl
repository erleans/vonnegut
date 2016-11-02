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

    vg:write(Topic, [crypto:strong_rand_bytes(60), crypto:strong_rand_bytes(60),
                     crypto:strong_rand_bytes(6), crypto:strong_rand_bytes(6),
                     crypto:strong_rand_bytes(60)]),

    %% replica_id max_wait_time min_bytes [topics]
    %% topic [partitions]
    %% partition fetch_offset max_bytes

    %% ReplicaId:32/signed, MaxWaitTime:32/signed, MinBytes:32/signed, Topics/binary

    %% api_key api_version correlation_id client_id
    Header = <<1:16/signed, 1:16/signed, 1:32/signed, 9:32/signed, "ci_client">>,

    Partitions = <<0:32/signed, 0:64/signed, 100:32/signed>>,
    Topics = <<10:32/signed, "test_topic", 1:32/signed, Partitions/binary>>,
    FetchReq = <<Header/binary, -1:32/signed, 1:32/signed, 100:32/signed, 1:32/signed, Topics/binary>>,
    Size = byte_size(FetchReq),

    Req = <<Size:32/signed, FetchReq/binary>>,

    {ok, Sock} = gen_tcp:connect("127.0.0.1", 5555, [binary]),
    ok = gen_tcp:send(Sock, Req),
    ok = inet:setopts(Sock, [{active, once}]),
    receive
        {tcp, Sock, Data} ->
            ct:pal("D ~p~n", [Data])
    end,
    ok = gen_tcp:close(Sock),

    ok.

-module(z_cluster_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("vg.hrl").

-define(NUM_NODES, 3).

suite() ->
    [{timetrap,{minutes,30}}].

init_per_suite(Config) ->
    application:stop(partisan),
    application:stop(vonnegut),
    application:unload(vonnegut),
    application:load(vonnegut),
    application:set_env(vonnegut, client_pool_size, 2),

    %% start several nodes
    Nodes0 =
        [begin
             Name = test_node(integer_to_list(N0)),
             Args = make_args(false, N0),
             IArgs = make_args(true, N0),
             {ok, HostNode} = ct_slave:start(Name, Args),
             {HostNode, {Name, {Args, IArgs}}}
         end
         || N0 <- lists:seq(0, ?NUM_NODES - 1)],
    {Nodes, RestartInfos} = lists:unzip(Nodes0),
    wait_for_nodes(Nodes, 50), % wait a max of 5s
    timer:sleep(3000),

    {ok, CoverNodes} = ct_cover:add_nodes(Nodes),
    ct:pal("cover added to ~p", [CoverNodes]),
    [{nodes, Nodes},
     {restart, RestartInfos}
     | Config].

make_args(Inverse, N0) ->
    N = integer_to_list(N0),

    ErlFlags = "-config ../../../../cluster/sys.config",

    CodePath = code:get_path(),

    Chain = case Inverse of
                true ->
                    chain(5555 + N0);
                false ->
                    inverse_chain(5557 - N0)
            end,
    [{kill_if_fail, true},
     {monitor_master, true},
     {init_timeout, 3000},
     {startup_timeout, 3000},
     {startup_functions,
      [{code, set_path, [CodePath]},
       {application, load, [lager]},
       {application, set_env,
        [lager, handlers,
         [{lager_console_backend, [{level, debug}]},
          {lager_file_backend,
           [{file, "log/console"++N++".log"}, {level, debug}]}]]},
       {application, ensure_all_started, [lager]},
       {application, load, [partisan]},
       {application, set_env, [partisan, peer_ip, {127,0,0,1}]},
       {application, set_env, [partisan, peer_port, 15555+N0]},
       {application, set_env, [partisan, gossip_interval, 500]},
       {application, load, [vonnegut]},
       {application, set_env, [vonnegut, chain, Chain]},
       {application, set_env, [vonnegut, client_pool_size, 2]},
       {application, set_env, [vonnegut, http_port, 8000+N0]},
       {application, set_env,
        [vonnegut, log_dirs, ["data/node" ++ N ++ "/"]]},
       {application, ensure_all_started, [vonnegut]}]},
     {erl_flags, ErlFlags}].


wait_for_nodes(_Nodes, 0) ->
    error(too_slow);
wait_for_nodes(Nodes, N) ->
    case lists:usort([net_adm:ping(Node) || Node <- Nodes]) of
        [pong] ->
            ok;
        _ ->
            timer:sleep(100),
            wait_for_nodes(Nodes, N - 1)
    end.

test_node(N) ->
    list_to_atom("chain1-" ++ N ++ "@127.0.0.1").

chain(N) ->
    [{name, chain1},
     {discovery, {direct, [{'chain1-0', "127.0.0.1", 15555, 5555},
                           {'chain1-1', "127.0.0.1", 15556, 5556},
                           {'chain1-2', "127.0.0.1", 15557, 5557}
                          ]}},
     {replicas, ?NUM_NODES},
     {port, N}].

inverse_chain(N) ->
    [{name, chain1},
     {discovery, {direct, [{'chain1-0', "127.0.0.1", 15555, 5557},
                           {'chain1-1', "127.0.0.1", 15556, 5556},
                           {'chain1-2', "127.0.0.1", 15557, 5555}
                          ]}},
     {replicas, ?NUM_NODES},
     {port, N}].

end_per_suite(Config) ->
    Nodes = ?config(nodes, Config),
    [begin
         ct_slave:stop(Node)
     end
     || {_Pid, Node} <- Nodes],
    application:stop(vonnegut),
    application:unload(vonnegut),
    net_kernel:stop(),
    application:ensure_all_started(vonnegut),
    ok.

connect(Node, _Wait, 0) ->
    lager:error("could not connect to ~p, exiting", [Node]),
    exit(disterl);
connect(Node, Wait, Tries) ->
    try
        pong = net_adm:ping(Node)
    catch _:_ ->
            lager:debug("connect failed: ~p ~p", [Node, Tries]),
            timer:sleep(Wait),
            connect(Node, Wait, Tries - 1)
    end.

init_per_group(_, Config) ->
    Config.

end_per_group(_, _) ->
    ok.

init_per_testcase(roles_w_reconnect, Config) ->
    application:load(vonnegut),
    application:start(shackle),
    %% wait for the cluster manager to be up before starting the pools
    case wait_for_mgr() of
        ok ->
            application:set_env(vonnegut, client, [{endpoints, [{"127.0.0.1", 5555}]}]),
            %% start so we can test we get the errors first
            application:set_env(vonnegut, swap_restart, false),
            ok = vg_client_pool:start(),
            timer:sleep(750),
            Config;
        _ ->
            error(vg_cluster_mgr_timeout)
    end;
init_per_testcase(_TestCase, Config) ->
    application:load(vonnegut),
    application:start(shackle),
    %% wait for the cluster manager to be up before starting the pools
    case wait_for_mgr() of
        ok ->
            application:set_env(vonnegut, client, [{endpoints, [{"127.0.0.1", 5555}]}]),
            ok = vg_client_pool:start(#{reconnect => false}),
            timer:sleep(750),
            Config;
        _ ->
            error(vg_cluster_mgr_timeout)
    end.

end_per_testcase(_TestCase, Config) ->
    application:unload(vonnegut),
    vg_client_pool:stop(),
    application:stop(shackle),
    Config.

wait_for_mgr() ->
    wait_for_mgr(0).

wait_for_mgr(N) when N =:= 50 ->
    timeout;
wait_for_mgr(N) ->
    case rpc:call('chain1-0@127.0.0.1', erlang, whereis, [vg_cluster_mgr]) of
        undefined ->
            timer:sleep(50),
            wait_for_mgr(N+1);
        _ ->
            ok
    end.

groups() ->
    [
     {init,
      [],
      [
       bootstrap,
       healthcheck
      ]},
     {log,
      [],
      [
       roles,
       concurrent_fetch,
       id_replication,
       restart,
       %% this needs to be at the end, I think, or it might hork the
       %% rest of the tests
       roles_w_reconnect
      ]},
     {ops,
      [],
      [
       deactivate_list_topics%% ,
       %% describe_topic
      ]}
    ].

all() ->
    [
     {group, init},
     {group, log},
     {group, ops}
    ].

%% test that the cluster is up and can do an end-to-end write/read cycle
bootstrap(Config) ->
    %% just create topics when written to for now
    ok = wait_for_start(fun() -> vg_client:ensure_topic(<<"foo">>) end),

    {ok, R} = vg_client:produce(<<"foo">>, <<"bar">>),
    {ok, R1} = vg_client:fetch(<<"foo">>),
    ct:pal("r ~p ~p", [R, R1]),
    ?assertMatch(#{<<"foo">> := #{0 := #{record_batches := [#{value := <<"bar">>}]}}}, R1),
    Config.

healthcheck(Config) ->
    [begin
         ?assertMatch({ok,{{"HTTP/1.1", 200, "OK"},
                           [{"connection", "Keep-Alive"},
                            {"content-length", "14"}],
                           "It's all good."}},
                      httpc:request("http://127.0.0.1:800" ++ integer_to_list(N) ++ "/_health")),
         ?assertMatch({ok,{{"HTTP/1.1", 404, "Not Found"},
                           [{"connection", "Keep-Alive"},
                            {"content-length", "9"}],
                           "Not Found"}},
                      httpc:request("http://127.0.0.1:800" ++ integer_to_list(N)))

     end
     || N <- lists:seq(0, ?NUM_NODES - 1)],
    Config.

roles(Config) ->
    Topic = <<"foo">>,
    {ok, _} = vg_client:ensure_topic(Topic),
    %% write some stuff to have something to read.
    [vg_client:produce(Topic, <<"bar", (integer_to_binary(N))/binary>>)
     || N <- lists:seq(1, 20)],
    timer:sleep(100),
    {ok, #{Topic := #{0 := #{record_batches := _ }}}} = vg_client:fetch(Topic),

    %% try to do a read on the head
    {ok, WritePool} = vg_client_pool:get_pool(Topic, write),
    {ok, ReadPool} = vg_client_pool:get_pool(Topic, read),
    vg_client_pool:start_pool(middle_end, #{ip => "127.0.0.1", port => 5556,
                                            reconnect => false}),

    ReplicaId = -1,
    MaxWaitTime = 5000,
    MinBytes = 100,
    Request1 = vg_protocol:encode_fetch(ReplicaId, MaxWaitTime, MinBytes, [{Topic, [{0, 12, 100}]}]),
    {ok, R} = shackle:call(WritePool, {?FETCH_REQUEST, Request1}),
    ?assertMatch(#{Topic := #{0 := #{error_code := 129}}}, vg_protocol:decode_response(?FETCH_REQUEST, R)),

    Request2 = vg_protocol:encode_fetch(ReplicaId, MaxWaitTime, MinBytes, [{Topic, [{0, 12, 100, 13}]}]),
    {ok, R2} = shackle:call(WritePool, {?FETCH2_REQUEST, Request2}),
    ?assertMatch(#{Topic := #{0 := #{error_code := 129}}}, vg_protocol:decode_response(?FETCH2_REQUEST, R2)),

    %% try to do a write on the tail
    #{record_batch := RecordBatch0} = vg_protocol:encode_record_batch([<<"bar3000">>, <<"barn_owl">>]),
    Acks = 0,
    Timeout = 5000,
    Request3 = vg_protocol:encode_produce(Acks, Timeout, [{Topic, [{0, RecordBatch0}]}]),
    {ok, R1} =  shackle:call(ReadPool, {?PRODUCE_REQUEST, Request3}),
    ?assertMatch(#{Topic := #{0 := #{error_code := 131}}}, vg_protocol:decode_response(?PRODUCE_REQUEST, R1)),

    %% try to do a read and write the middle of the chain
    Request4 = vg_protocol:encode_fetch(ReplicaId, MaxWaitTime, MinBytes, [{Topic, [{0, 12, 100}]}]),
    {ok, Ret} = shackle:call(middle_end, {?FETCH_REQUEST, Request4}),
    ?assertMatch(#{Topic := #{0 := #{error_code := 129}}}, vg_protocol:decode_response(?FETCH_REQUEST, Ret)),
    #{record_batch := RecordBatch1} = vg_protocol:encode_record_batch([<<"bar3000">>, <<"barn_owl">>]),
    Request5 = vg_protocol:encode_produce(Acks, Timeout, [{Topic, [{0, RecordBatch1}]}]),
    {ok, Ret2} =  shackle:call(middle_end, {?PRODUCE_REQUEST, Request5}),
    ?assertMatch(#{Topic := #{0 := #{error_code := 131}}}, vg_protocol:decode_response(?PRODUCE_REQUEST, Ret2)),

    shackle_pool:stop(middle_end),
    Config.

roles_w_reconnect(Config) ->
    Nodes = ?config(nodes, Config),
    Restarts = ?config(restart, Config),

    [begin
         %% make a lot of topics and add a few things to them to delay restart
         Topic = <<"bar", (integer_to_binary(N))/binary>>,
         {ok, _} = vg_client:ensure_topic(Topic),
         vg_client:produce(Topic, [<<"baz", (integer_to_binary(T))/binary>>
                                       || T <- lists:seq(1, 50)])
     end
     || N <- lists:seq(1, 20)],

    [begin
         ?assertMatch({ok, _}, ct_slave:stop(Node))
     end
     || Node <- Nodes],
    ct:pal("nodes() ~p", [nodes()]),
    timer:sleep(1000),
    %% restart the nodes with the ports inverted so we'll reconnect to
    %% the wrong nodes and get disallowed errors.  We can test failure
    %% and reconnect modes.
    Nodes1 =
        [begin
             {ok, Node} = ct_slave:start(Name, IArgs),
             Node
         end
         || {Name, {_Args, IArgs}} <- Restarts],

    _ = ct_cover:add_nodes(Nodes1),

    wait_for_nodes(Nodes1, 50), % wait a max of 5s
    timer:sleep(2000),

    ?assertMatch({error, 131}, vg_client:produce(<<"bar4">>, <<"ASDASDASDASDASDASDA">>)),
    %% not sure that I like the inconsistency here
    ?assertMatch({ok, #{<<"bar9">> := #{0 := #{error_code := 129}}}},
                 vg_client:fetch(<<"bar9">>, 0, 1)),

    %% set reconnection on, it's checked dynamically
    application:set_env(vonnegut, swap_restart, true),

    ?assertMatch({ok, 100}, vg_client:produce(<<"bar9">>, <<"ASDASDASDASDASDASDA">>)),
    ?assertMatch({ok, #{<<"bar9">> := #{0 := #{error_code := 0}}}},
                  vg_client:fetch(<<"bar9">>, 0, 1)),
    Config.

concurrent_fetch(_Config) ->
    Topic = vg_test_utils:create_random_name(<<"cluster_concurrent_fetch">>),
    {ok, _} = vg_client:ensure_topic(Topic),
    RandomRecords = [crypto:strong_rand_bytes(60), crypto:strong_rand_bytes(60),
                     crypto:strong_rand_bytes(6), crypto:strong_rand_bytes(6),
                     crypto:strong_rand_bytes(60)],
    [vg_client:produce(Topic, RandomRecords) || _ <- lists:seq(0, 100)],
    Self = self(),
    [spawn(fun() ->
               timer:sleep(50),
               try
                   [?assertMatch({ok, _}, vg_client:fetch(Topic)) || _ <- lists:seq(0, 10)],
                   Self ! {done, N}
               catch
                   C:T ->
                       ct:pal("~p ~p ~p", [C, T, erlang:get_stacktrace()]),
                       Self ! fail
               end
          end) || N <- lists:seq(0,  10)],

    receive
        {done, 10} ->
            ok;
        fail ->
            throw(fail)
    after
        100000 ->
            throw(timeout)
    end.

restart(Config) ->
    %% restart to get reconnection behavior
    ok = vg_client_pool:stop(),
    ok = vg_client_pool:start(),
    Nodes = ?config(nodes, Config),
    Restarts = ?config(restart, Config),

    %% add some stuff to look at
    ct:pal("nodes ~p", [Nodes]),
    [begin
         %% make a lot of topics and add a few things to them to delay restart
         Topic = <<"bar", (integer_to_binary(N))/binary>>,
         {ok, _} = vg_client:ensure_topic(Topic),
         vg_client:produce(Topic, [<<"baz", (integer_to_binary(T))/binary>>
                                       || T <- lists:seq(1, 50)])
     end
     || N <- lists:seq(1, 20)],

    %% test whole cluster restart
    Nodes1 = restart_cluster(Config),

    timer:sleep(2000),
    ok = wait_for_start(fun() -> vg_client:fetch(<<"bar4">>, 0, 1) end),

    %% probably need a better test here
    {ok, Result} = vg_client:fetch(<<"bar4">>, 0, 1000),
    ?assertMatch(#{<<"bar4">> := #{0 := #{high_water_mark := 49}}},
                 Result),

    %% shut down each node in turn
    [Head, Middle, Tail] = Nodes1,
    [{HeadName, {HeadArgs, _}},
     {MiddleName, {MiddleArgs, _}},
     {TailName, {TailArgs, _}}] = Restarts,

    {ok, _} = ct_slave:stop(Head),
    timer:sleep(1000),
    ?assertMatch({ok, #{<<"bar4">> := #{0 := #{high_water_mark := 49}}}}, vg_client:fetch(<<"bar4">>, 0, 1000)),
    ?assertMatch({error, pool_timeout}, vg_client:produce(<<"bar4">>, <<"this should fail">>)),
    {ok, HeadNode} = ct_slave:start(HeadName, HeadArgs),
    timer:sleep(100),

    _ = ct_cover:add_nodes([HeadNode]),

    %% if we start getting off-by-one errors in the hwms below, I
    %% suspect that we might have gotten a timeout below and then
    %% redid the produce
    ok = wait_for_start(fun() -> vg_client:produce(<<"bar4">>, <<"this should work eventually">>) end),
    ?assertMatch({ok, #{<<"bar4">> := #{0 := #{high_water_mark := 50}}}}, vg_client:fetch(<<"bar4">>, 0, 1000)),

    {ok, _} = ct_slave:stop(Middle),
    timer:sleep(1000),
    ?assertMatch({ok, #{<<"bar4">> := #{0 := #{high_water_mark := 50}}}}, vg_client:fetch(<<"bar4">>, 0, 1000)),
    ?assertMatch({error, timeout}, vg_client:produce(<<"bar4">>, <<"this should fail too">>)),
    {ok, MiddleNode} = ct_slave:start(MiddleName, MiddleArgs),
    timer:sleep(100),

    _ = ct_cover:add_nodes([MiddleNode]),

    ok = wait_for_start(fun() -> vg_client:produce(<<"bar4">>, <<"this should work eventually too">>) end),
    ?assertMatch({ok, #{<<"bar4">> := #{0 := #{high_water_mark := 51}}}}, vg_client:fetch(<<"bar4">>, 0, 1000)),

    {ok, _} = ct_slave:stop(Tail),
    timer:sleep(1000),
    ?assertMatch({error, pool_timeout}, vg_client:fetch(<<"bar4">>, 0, 1000)),
    ?assertMatch({error, timeout}, vg_client:produce(<<"bar4">>, <<"this should fail">>)),
    {ok, TailNode} = ct_slave:start(TailName, TailArgs),
    timer:sleep(100),

    _ = ct_cover:add_nodes([TailNode]),
    ok = wait_for_start(fun() -> vg_client:fetch(<<"bar4">>, 0, 1) end),
    ?assertMatch({ok, #{<<"bar4">> := #{0 := #{high_water_mark := 51}}}}, vg_client:fetch(<<"bar4">>, 0, 1000)),
    ?assertMatch({ok, 52}, vg_client:produce(<<"bar4">>, <<"this should work">>)),

    Config.

id_replication(Config) ->
    Nodes = ?config(nodes, Config),
    [_Head, _Middle, Tail] = Nodes,

    Topic = <<"bar-n">>,
    {ok, _} = vg_client:ensure_topic(Topic),
    ?assertMatch({ok, 49}, vg_client:produce(Topic, [<<"baz", (integer_to_binary(T))/binary>>
                                                         || T <- lists:seq(1, 50)])),

    ok = wait_for_start(fun() -> vg_client:fetch(Topic, 0, 1) end),

    ?assertMatch({ok, 50}, vg_client:produce(Topic, <<"this should work">>)),

    %% this block is kind of fragile, in the future we might want to
    %% build this sort of stuff into the vg.erl client when it's
    %% rewritten to separate conn and reading/writing from disk

    %% what it's doing is simulating a situation where tail receives a
    %% write and commits it, but then either middle or head are
    %% partitioned, so have not written it.  this means that the id of
    %% head and tail are out of sync, so we need to repair this
    %% somehow, so pass it back up to the head, which writes it then
    %% retries.  This is complicated by retry detection and multiple
    %% producers, but that should go away once we're on the 1.0
    %% protocol.
    RB=#{record_batch := RecordBatch} = vg_protocol:encode_record_batch([<<"asdasd">>, <<"asdasdas">>]),
    R0 = vg_protocol:encode_produce(0, 4000, [{Topic, [{0, RecordBatch}]}]),
    R1 = iolist_to_binary(R0),
    {_, _, _R2} = vg_protocol:decode_produce_request(R1),
    ?assertMatch({ok, 52}, rpc:call(Tail, gen_server, call, [{via,gproc,{n,l,{active, <<"bar-n">>, 0}}}, {write, 53, RB}, 5000])),
    %% note that we VVVV succeed with 53 even though the previous writes never went through head
    ?assertMatch({ok, 53}, vg_client:produce(Topic, <<"this should succeed on retry">>)),
    ct:pal("~p", [vg_client:fetch(Topic, 50, 10)]),
    Config.

deactivate_list_topics(Config) ->
    %% list topics, get the topic from the prior test
    [Head | _ ] = ?config(nodes, Config),

    InitTopics = rpc:call(Head, vg, running_topics, []),
    ?assert(length(InitTopics) =< 1),

    %% make a few topics
    [begin
         %% make a lot of topics and add a few things to them to delay restart
         Topic = <<"quux", (integer_to_binary(N))/binary>>,
         {ok, _} = vg_client:ensure_topic(Topic),
         vg_client:produce(Topic, [<<"baz", (integer_to_binary(T))/binary>>
                                       || T <- lists:seq(1, 50)])
     end
     || N <- lists:seq(1, 20)],

    %% list them, make sure they're all there
    Topics = rpc:call(Head, vg, running_topics, []),
    [begin
         Topic = <<"quux", (integer_to_binary(N))/binary>>,
         ?assertEqual(true, lists:member({Topic, 0}, Topics))
     end
     || N <- lists:seq(1, 20)],

    %% deactivate a subset
    [[ok] = rpc:call(Head, vg, deactivate_topic, [<<"quux", (integer_to_binary(N))/binary>>])
     || N <- lists:seq(1, 10)],

    %% list again, make sure they're shut down
    Topics2 = rpc:call(Head, vg, running_topics, []),
    [begin
         Topic = <<"quux", (integer_to_binary(N))/binary>>,
         ?assertEqual(true, lists:member({Topic, 0}, Topics2))
     end
     || N <- lists:seq(11, 20)],
    [begin
         Topic = <<"quux", (integer_to_binary(N))/binary>>,
         ?assertEqual(false, lists:member({Topic, 0}, Topics2))
     end
     || N <- lists:seq(1, 10)],

    %% restart the cluster
    [Head1 | _ ] = restart_cluster(Config),
    ct:pal("head1 ~p", [Head1]),
    timer:sleep(1000),

    %% make sure they're gone
    ?assertEqual([],
                 rpc:call(Head1, vg, running_topics, [])),

    %% write to a subset to restart them
    [begin
         Topic = <<"quux", (integer_to_binary(N))/binary>>,
         Foo = vg_client:produce(Topic, <<"blerp">>),
         ct:pal("foo ~p", [Foo]),
         ?assertMatch({ok, _}, Foo)
     end
     || N <- lists:seq(11, 20)],

    %% list topics to make sure only they are started
    Topics3 = rpc:call(Head1, vg, running_topics, []),
    ct:pal("topics3 ~p", [Topics3]),
    [begin
         Topic = <<"quux", (integer_to_binary(N))/binary>>,
         ?assertEqual(true, lists:member({Topic, 0}, Topics3))
     end
     || N <- lists:seq(11, 20)],
    [begin
         Topic = <<"quux", (integer_to_binary(N))/binary>>,
         ?assertEqual(false, lists:member({Topic, 0}, Topics3))
     end
     || N <- lists:seq(1, 10)],

    %% delete a subset including some that are not active
    [ok = rpc:call(Head, vg, delete_topic, [<<"quux", (integer_to_binary(N))/binary>>])
     || N <- lists:seq(5, 14)],

    %% make sure they're not running anymore via list and fetch
    Topics4 = rpc:call(Head1, vg, running_topics, []),
    ct:pal("topics3 ~p", [Topics4]),
    [begin
         Topic = <<"quux", (integer_to_binary(N))/binary>>,
         ?assertEqual(true, lists:member({Topic, 0}, Topics4))
     end
     || N <- lists:seq(15, 20)],
    [begin
         Topic = <<"quux", (integer_to_binary(N))/binary>>,
         ?assertEqual(false, lists:member({Topic, 0}, Topics4))
     end
     || N <- lists:seq(1, 14)],

    ?assertMatch({ok, 50}, vg_client:produce(<<"quux2">>, <<"blerp2">>)),
    ?assertMatch({ok, 0}, vg_client:produce(<<"quux13">>, <<"blerp2">>)),
    ?assertMatch({ok, 0} , vg_client:produce(<<"quux6">>, <<"blerp2">>)),
    ?assertMatch({error,{<<"quux12">>,not_found}}, vg_client:fetch(<<"quux12">>, 10, 1)),
    ?assertMatch({error,{<<"quux7">>,not_found}}, vg_client:fetch(<<"quux7">>, 10, 2)),

    Config.

%%%%
%%%%  UTILITIES
%%%%

wait_for_start(Thunk) ->
    wait_for_start(Thunk, 5000).

wait_for_start(_, Zero) when Zero =< 0 ->
    {error, waited_too_long};
wait_for_start(Thunk, N) ->
    case Thunk() of
        {ok, _} = _OK ->
            %% ct:pal("ok ~p", [_OK]),
            timer:sleep(150),
            ok;
        {error, E} when E =:= no_socket orelse
                        E =:= socket_closed ->
            timer:sleep(1),
            wait_for_start(Thunk, N - 1);
        {error, E} when E =:= pool_timeout orelse
                        E =:= timeout ->
            wait_for_start(Thunk, N - 1000)
    end.


%% to run this test: rebar3 ct --suite=z_cluster_SUITE --case=concurrent_perf
concurrent_perf(_Config) ->
    Topic1 = vg_test_utils:create_random_name(<<"cluster_concurrent_perf1">>),
    Topic2 = vg_test_utils:create_random_name(<<"cluster_concurrent_perf2">>),
    Topic3 = vg_test_utils:create_random_name(<<"cluster_concurrent_perf3">>),

    {ok, _} = vg_client:ensure_topic(Topic1),
    {ok, _} = vg_client:ensure_topic(Topic2),
    {ok, _} = vg_client:ensure_topic(Topic3),

    RandomRecords = [crypto:strong_rand_bytes(60), crypto:strong_rand_bytes(60),
                     crypto:strong_rand_bytes(6), crypto:strong_rand_bytes(6),
                     crypto:strong_rand_bytes(60)],
    RandomBigRecords = [crypto:strong_rand_bytes(6000), crypto:strong_rand_bytes(60000),
                        crypto:strong_rand_bytes(600), crypto:strong_rand_bytes(600),
                        crypto:strong_rand_bytes(6000)],
    Scale = 1000,
    LoadStart = erlang:monotonic_time(milli_seconds),
    [vg_client:produce(Topic1, RandomRecords) || _ <- lists:seq(1, Scale)],
    [vg_client:produce(Topic2, RandomRecords) || _ <- lists:seq(1, Scale)],
    [vg_client:produce(Topic3, RandomBigRecords) || _ <- lists:seq(1, Scale)],

    Pids = 12,
    Self = self(),
    LoadEnd = erlang:monotonic_time(milli_seconds),
    timer:sleep(2000),
    RetrieveStart = erlang:monotonic_time(milli_seconds),
    [begin
         F = fun(ID) ->
                     %%timer:sleep(50),
                     try
                         Topic = case ID rem 3 of
                                     0 -> Topic1;
                                     1 -> Topic2;
                                     2 -> Topic3
                                 end,
                         %%[
                         FetchStart = erlang:monotonic_time(milli_seconds),
                         {ok, #{record_batches := L}} = vg_client:fetch(Topic, 0, 60000),
                         FetchEnd = erlang:monotonic_time(milli_seconds),
                         ?assertEqual(Scale * length(RandomRecords), length(L)),
                         %%?assertEqual(500, length(L)),
                         %%|| _ <- lists:seq(0, 10)],
                         Self ! done,
                         io:fwrite(standard_error, "thread ~p fetch on topic ~p completed in ~p ms~n",
                                   [ID, Topic, FetchEnd - FetchStart])
                     catch
                         C:T ->
                             ct:pal("~p ~p ~p", [C, T, erlang:get_stacktrace()]),
                             Self ! fail
                     end
             end,
         spawn(fun() -> F(N) end)
     end || N <- lists:seq(0,  Pids)],

    [receive
         done -> ok;
         fail -> throw(fail)
     after
         1000000 -> throw(timeout)
     end || _ <- lists:seq(0, Pids)],
    RetrieveEnd = erlang:monotonic_time(milli_seconds),
    LoadDiff = LoadEnd - LoadStart,
    RetDiff = RetrieveEnd - RetrieveStart,
    io:fwrite(standard_error, "load ~p ret ~p ~n ~n", [LoadDiff, RetDiff]),
    %%throw(gimmelogs),
    ok.

sync_cmd(Name, Cmd, Env) ->
    P = open_port({spawn, Cmd},
                  [{env, Env},
                   stream, use_stdio,
                   exit_status,
                   stderr_to_stdout]),
    loop(Name, P).

cmd(Name, Cmd, Env) ->
    spawn(fun() ->
                  P = open_port({spawn, Cmd},
                                [{env, Env},
                                 stream, use_stdio,
                                 exit_status,
                                 stderr_to_stdout]),
                  loop(Name, P)
          end).

loop(Name, Port) ->
    receive
        stop ->
            stop;
        {Port, {data, Data}} ->
            ct:pal("~p: port data ~p ~p", [Name, Port, Data]),
            loop(Name, Port);
        {Port, {exit_status, Status}} ->
            case Status of
                0 ->
                    ok; %% ct:pal("port exit ~p ~p", [Port, Status]);
                _ ->
                    ct:pal("port exit ~p ~p", [Port, Status])
            end,
            Status
    end.

%% multicall shorthand for when nodes() is OK (i.e. all nodes other
%% than the test runner).
do(M, F, A) ->
    {_, []} = rpc:multicall(nodes(), M, F, A).

nop(Config) ->
    Config.

restart_cluster(Config) ->
    Nodes = ?config(nodes, Config),
    Restarts = ?config(restart, Config),
    ct:pal("nodes() ~p", [nodes()]),
    [begin
         ?assertMatch({ok, _}, ct_slave:stop(Node))
     end
     || Node <- Nodes],
    ct:pal("nodes() ~p", [nodes()]),
    timer:sleep(1000),
    Nodes1 =
        [begin
             {ok, Node} = ct_slave:start(Name, Args),
             Node
         end
         || {Name, {Args, _}} <- Restarts],

    _ = ct_cover:add_nodes(Nodes1),

    wait_for_nodes(Nodes1, 50), % wait a max of 5s
    Nodes1.

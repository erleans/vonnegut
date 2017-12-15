-module(z_cluster_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

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
    ErlFlags = "-config ../../../../cluster/sys.config",

    CodePath = code:get_path(),
    Nodes0 =
        [begin
             N = integer_to_list(N0),
             Name = test_node(N),
             Args = [{kill_if_fail, true},
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
                       {application, set_env, [vonnegut, chain, chain(5555+N0)]},
                       {application, set_env, [vonnegut, client_pool_size, 2]},
                       {application, set_env, [vonnegut, http_port, 8000+N0]},
                       {application, set_env,
                        [vonnegut, log_dirs, ["data/node" ++ N ++ "/"]]},
                       {application, ensure_all_started, [vonnegut]}]},
                     {erl_flags, ErlFlags}],
             {ok, HostNode} = ct_slave:start(Name, Args),
             {HostNode, {Name, Args}}
         end
         || N0 <- lists:seq(0, ?NUM_NODES - 1)],
    {Nodes, RestartInfos} = lists:unzip(Nodes0),
    wait_for_nodes(Nodes, 50), % wait a max of 5s
    timer:sleep(3000),
    swap_lager(Nodes),
    {ok, CoverNodes} = ct_cover:add_nodes(Nodes),
    ct:pal("cover added to ~p", [CoverNodes]),
    [{nodes, Nodes},
     {restart, RestartInfos}
     | Config].

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

start_lager() ->
    application:load(lager),
    application:set_env(lager, colored, true),
    application:set_env(lager, handlers,
                        [{lager_file_backend,
                          [{file, "console.log"}, {level, debug}]},
                         {lager_console_backend,
                          [{level, info},
                           {formatter, lager_default_formatter},
                           {formatter_config,
                            [time, color, " [",severity,"] ", pid, " ",
                             module, ":", function, ":", line, ":",
                             message, "\n"]}]}]),
    application:ensure_all_started(lager),
    ok.

swap_lager(Nodes) ->
    %% for deeper debugging, sometimes it's nice to be able to toggle this off
    Disable = true,
    case Disable of
        true ->
            ok;
        _ ->
            [begin
                 pong = net_adm:ping(Node),
                 ok = rpc:call(Node, vonnegut_app, swap_lager, [whereis(lager_event)])
             end
             || Node <- Nodes]
    end.

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

init_per_testcase(_TestCase, Config) ->
    application:load(vonnegut),
    application:start(shackle),
    %% start_lager(),
    %% swap_lager(?config(nodes, Config)),
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
    case global:whereis_name(vg_cluster_mgr) of
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
       bootstrap
      ]},
     {operations,
      [],
      [
       roles,
       acks,
       concurrent_fetch,
       id_replication,
       restart
      ]}

    ].

all() ->
    [
     {group, init},
     {group, operations}
    ].

%% test that the cluster is up and can do an end-to-end write/read cycle
bootstrap(Config) ->
    %% just create topics when written to for now
    ok = wait_for_start(fun() -> vg_client:ensure_topic(<<"foo">>) end),

    {ok, R} = vg_client:produce(<<"foo">>, <<"bar">>),
    {ok, R1} = vg_client:fetch(<<"foo">>),
    ct:pal("r ~p ~p", [R, R1]),
    ?assertMatch(#{<<"foo">> := #{0 := #{record_set := [#{record := <<"bar">>}]}}}, R1),
    Config.

roles(Config) ->
    Topic = <<"foo">>,
    {ok, _} = vg_client:ensure_topic(Topic),
    %% write some stuff to have something to read.
    [vg_client:produce(Topic, <<"bar", (integer_to_binary(N))/binary>>)
     || N <- lists:seq(1, 20)],
    timer:sleep(100),
    {ok, #{<<"foo">> := #{0 := #{record_set := _ }}}} = vg_client:fetch(Topic),

    %% try to do a read on the head
    {ok, WritePool} = vg_client_pool:get_pool(Topic, write),
    {ok, ReadPool} = vg_client_pool:get_pool(Topic, read),
    vg_client_pool:start_pool(middle_end, #{ip => "127.0.0.1", port => 5556,
                                            reconnect => false}),

    {ok, R} = shackle:call(WritePool, {fetch, [{Topic, [{0, 12, 100}]}]}),
    ?assertMatch(#{Topic := #{0 := #{error_code := 129}}}, R),

    {ok, R2} = shackle:call(WritePool, {fetch2, [{Topic, [{0, 12, 100, 13}]}]}),
    ?assertMatch(#{Topic := #{0 := #{error_code := 129}}}, R2),

    %% try to do a write on the tail
    {ok, R1} =  shackle:call(ReadPool, {produce, [{Topic, [{0, [<<"bar3000">>, <<"barn_owl">>]}]}]}),
    ?assertMatch(#{Topic := #{0 := #{error_code := 131}}}, R1),

    %% try to do a read and write the middle of the chain
    {ok, Ret} = shackle:call(middle_end, {fetch, [{Topic, [{0, 12, 100}]}]}),
    ?assertMatch(#{Topic := #{0 := #{error_code := 129}}}, Ret),
    {ok, Ret2} =  shackle:call(middle_end, {produce, [{Topic, [{0, [<<"bar3000">>, <<"barn_owl">>]}]}]}),
    ?assertMatch(#{Topic := #{0 := #{error_code := 131}}}, Ret2),

    shackle_pool:stop(middle_end),
    Config.

acks(Config) ->
    {ok, _} = vg_client:ensure_topic(<<"foo">>),
    [vg_client:produce(<<"foo">>, <<"bar", (integer_to_binary(N))/binary>>)
     || N <- lists:seq(1, 20)],

    ?assertMatch(ok, check_acks(1000)),
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

    %% test whole node restart
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
         || {Name, Args} <- Restarts],
    swap_lager(Nodes1),
    _ = ct_cover:add_nodes(Nodes1),

    wait_for_nodes(Nodes1, 50), % wait a max of 5s
    timer:sleep(2000),
    ok = wait_for_start(fun() -> vg_client:fetch(<<"bar4">>, 0, 1) end),

    %% probably need a better test here
    {ok, Result} = vg_client:fetch(<<"bar4">>, 0, 1000),
    ?assertMatch(#{<<"bar4">> := #{0 := #{high_water_mark := 49}}},
                 Result),

    %% shut down each node in turn
    [Head, Middle, Tail] = Nodes1,
    [{HeadName, HeadArgs},
     {MiddleName, MiddleArgs},
     {TailName, TailArgs}] = Restarts,

    {ok, _} = ct_slave:stop(Head),
    timer:sleep(1000),
    ?assertMatch({ok, #{<<"bar4">> := #{0 := #{high_water_mark := 49}}}}, vg_client:fetch(<<"bar4">>, 0, 1000)),
    ?assertMatch({error, pool_timeout}, vg_client:produce(<<"bar4">>, <<"this should fail">>)),
    {ok, HeadNode} = ct_slave:start(HeadName, HeadArgs),
    timer:sleep(100),
    swap_lager([HeadNode]),
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
    swap_lager([MiddleNode]),
    _ = ct_cover:add_nodes([MiddleNode]),

    ok = wait_for_start(fun() -> vg_client:produce(<<"bar4">>, <<"this should work eventually too">>) end),
    ?assertMatch({ok, #{<<"bar4">> := #{0 := #{high_water_mark := 51}}}}, vg_client:fetch(<<"bar4">>, 0, 1000)),

    {ok, _} = ct_slave:stop(Tail),
    timer:sleep(1000),
    ?assertMatch({error, pool_timeout}, vg_client:fetch(<<"bar4">>, 0, 1000)),
    ?assertMatch({error, timeout}, vg_client:produce(<<"bar4">>, <<"this should fail">>)),
    {ok, TailNode} = ct_slave:start(TailName, TailArgs),
    timer:sleep(100),
    swap_lager([TailNode]),
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
    vg_client:produce(Topic, [<<"baz", (integer_to_binary(T))/binary>>
                                  || T <- lists:seq(1, 50)]),

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
    R0 = vg_protocol:encode_produce(0, 4000, [{Topic, [{0, [<<"asdasd">>, <<"asdasdas">>]}]}]),
    R1 = iolist_to_binary(R0),
    {_, _, R2} = vg_protocol:decode_produce_request(R1),
    [{_, R3}] = R2,
    [{_, R}] = R3,
    ?assertMatch({ok, 52}, rpc:call(Tail, gen_server, call, [{'bar-n-0', Tail}, {write, 53, R}, 5000])),
    %% note that we VVVV succeed with 53 even though the previous writes never went through head
    ?assertMatch({ok, 53}, vg_client:produce(Topic, <<"this should succeed on retry">>)),
    Config.

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
                         {ok, #{record_set := L}} = vg_client:fetch(Topic, 0, 60000),
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

check_acks(Timeout) when Timeout =< 0 ->
    {error, timeout};
check_acks(Timeout) ->
    Info0 = rpc:call(mkname('chain1-0'), ets, info, [foo0, size]),
    Info1 = rpc:call(mkname('chain1-1'), ets, info, [foo0, size]),
    Info2 = rpc:call(mkname('chain1-2'), ets, info, [foo0, size]),
    ct:pal("ack info: ~p ~p ~p", [Info0, Info1, Info2]),

    case Info0 == 0 andalso Info1 == 0 andalso Info2 == 0 of
        true -> ok;
        _ ->
            timer:sleep(50),
            check_acks(Timeout - 50)
    end.

mkname(Name) ->
    binary_to_atom(
      iolist_to_binary(
        [atom_to_list(Name), "@127.0.0.1"]),
      utf8).

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

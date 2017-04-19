-module(cluster_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

suite() ->
    [{timetrap,{minutes,30}}].

init_per_suite(Config) ->
    lager:start(),
    %% make a release as test, should likely use better exit status
    %% stuff rather than assuming that it passes
    %% lager:info("~p", [os:cmd("pwd")]),
    0 = sync_cmd("release", "/bin/bash -c '(cd ../../../..; pwd; rebar3 as cluster release)'", []),

    %% make sure that the test runner has access to the env vars
    application:load(vonnegut),

    %% start several nodes
    Nodes =
        [begin
             %% fix the names to use the discovery stuff?
             N = integer_to_list(N0),
             Name = "chain1-" ++ N ++ "@127.0.0.1",
             ct:pal("N ~p ~p", [N, N0]),
             Port = integer_to_list(5555 + N0),
             PeerPort = integer_to_list(15555 + N0),
             Env = [{"RELX_REPLACE_OS_VARS", "true"},
                    {"LOG_DIR", "/tmp/vg-s"++ os:getpid() ++"/data/node" ++ N ++ "/"},
                    {"NODE", Name},
                    {"PORT", Port},
                    {"PEER_PORT", PeerPort}],
             Cmd = "../../../cluster/rel/vonnegut/bin/vonnegut console",
             Pid = cmd("node" ++ N, Cmd, Env),
             ct:pal("cmd ~p ~p", [Cmd, Env]),
             timer:sleep(1000),
             {Pid, list_to_atom(Name)}
         end
         || N0 <- lists:seq(0, 2)],

    %% Collect pids here (actually, using console + cmd() we don't
    %% need to, because console will exit when the testrunner exits).
    ct:pal("ps ~p", [os:cmd("ps aux | grep beam.sm[p]")]),

    %% give everything a bit to come up? TODO: proper wait
    timer:sleep(5000),

    %% establish disterl connections to each of them
    NodeName = 'testrunner@127.0.0.1',
    {ok, _Pid} = net_kernel:start([NodeName]),
    erlang:set_cookie(node(), vonnegut),
    [begin
         lager:info("attaching to ~p", [Node]),
         connect(Node, 50, 40)
     end
     || {_Pid1, Node} <- Nodes],
    [{nodes, Nodes}|Config].

end_per_suite(Config) ->
    Nodes = ?config(nodes, Config),
    [begin
         rpc:call(Node, erlang, halt, [0])
     end
     || {_Pid, Node} <- Nodes],
    ok.

init_per_group(_, Config) ->
    Config.

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

end_per_group(_GroupName, Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    application:load(vonnegut),
    application:start(shackle),
    %% wait for the cluster manager to be up before starting the pools
    case wait_for_mgr() of
        ok ->
            application:set_env(vonnegut, client, [{endpoints, [{"127.0.0.1", 5555}]}]),
            ok = vg_client_pool:start(),
            timer:sleep(3000),
            Config;
        _ ->
            {fail, vg_cluster_mgr_timeout}
    end.

wait_for_mgr() ->
    wait_for_mgr(0).

wait_for_mgr(N) when N =:= 5 ->
    timeout;
wait_for_mgr(N) ->
    case global:whereis_name(vg_cluster_mgr) of
        undefined ->
            timer:sleep(500),
            wait_for_mgr(N+1);
        _ ->
            ok
    end.

end_per_testcase(_TestCase, Config) ->
    application:unload(vonnegut),
    vg_client_pool:stop(),
    application:stop(shackle),
    Config.

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
       concurrent_fetch
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
    %% do(vg, create_topic, [<<"foo">>]),
    {ok, R} = vg_client:produce(<<"foo">>, <<"bar">>),
    timer:sleep(800),
    {ok, R1} = vg_client:fetch(<<"foo">>),
    ct:pal("r ~p ~p", [R, R1]),
    timer:sleep(1800),
    ?assertMatch(#{record_set := [#{record := <<"bar">>}]}, R1),
    Config.

roles(Config) ->
    Topic = <<"foo">>,
    %% write some stuff to have something to read.
    [vg_client:produce(Topic, <<"bar", (integer_to_binary(N))/binary>>)
     || N <- lists:seq(1, 20)],
    timer:sleep(800),
    vg_client:fetch(Topic),

    %% try to do a read on the head
    {ok, WritePool} = vg_client_pool:get_pool(Topic, write),
    {ok, ReadPool} = vg_client_pool:get_pool(Topic, read),

    {ok, R} = shackle:call(WritePool, {fetch, [{Topic, [{0, 12, 100}]}]}),
    timer:sleep(1000),
    ?assertMatch(#{Topic := #{0 := #{error_code := 129}}}, R),

    %% try to do a write on the tail
    {ok, R1} =  shackle:call(ReadPool, {produce, [{Topic, [{0, [<<"bar3000">>, <<"barn_owl">>]}]}]}),
    ?assertMatch(#{Topic := #{0 := #{error_code := 131}}}, R1),
    vg_client_pool:start_pool(middle_end, #{ip => "127.0.0.1", port => 5556}),

    %% try to connect the middle of the chain
    ?assertMatch({error, socket_closed},
                 shackle:call(middle_end, {fetch, [{Topic, [{0, 12, 100}]}]})),

    shackle_pool:stop(middle_end),
    Config.

acks(Config) ->
    [vg_client:produce(<<"foo">>, <<"bar", (integer_to_binary(N))/binary>>)
     || N <- lists:seq(1, 20)],

    ?assertMatch(ok, check_acks(1000)),

    Config.

concurrent_fetch(_Config) ->
    Topic = vg_test_utils:create_random_name(<<"cluster_concurrent_fetch">>),
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


%% to run this test: rebar3 ct --dir=cluster --sys_config=cluster/sys.config --suite=cluster_SUITE --case=concurrent_perf
concurrent_perf(_Config) ->
    Topic1 = vg_test_utils:create_random_name(<<"cluster_concurrent_perf1">>),
    Topic2 = vg_test_utils:create_random_name(<<"cluster_concurrent_perf2">>),
    Topic3 = vg_test_utils:create_random_name(<<"cluster_concurrent_perf3">>),

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

    Pids = 3,
    Self = self(),
    LoadEnd = erlang:monotonic_time(milli_seconds),
    timer:sleep(8000),
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
    Info0 = rpc:call('chain1-0@127.0.0.1', ets, info, [foo0, size]),
    Info1 = rpc:call('chain1-1@127.0.0.1', ets, info, [foo0, size]),
    Info2 = rpc:call('chain1-2@127.0.0.1', ets, info, [foo0, size]),
    ct:pal("ack info: ~p ~p ~p", [Info0, Info1, Info2]),

    case Info0 == 0 andalso Info1 == 0 andalso Info2 == 0 of
        true -> ok;
        _ ->
            timer:sleep(50),
            check_acks(Timeout - 50)
    end.

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

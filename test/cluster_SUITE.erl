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
    0 = sync_cmd("release", "/bin/bash -c '(cd ../../../..; pwd; rebar3 release)'", []),

    %% make sure that the test runner has access to the env vars
    ok = application:load(vonnegut),

    %% start several nodes
    Nodes =
        [begin
             %% fix the names to use the discovery stuff?
             N = integer_to_list(N0),
             Name = "chain1-" ++ N ++ "@127.0.0.1",
             Port = integer_to_list(5555 + N0),
             Env = [{"RELX_REPLACE_OS_VARS", "true"},
                    {"NODE", Name},
                    {"PORT", Port}],
             Cmd = "../../../default/rel/vonnegut/bin/vonnegut console",
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
    timer:sleep(15000),

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
        true = net_kernel:hidden_connect_node(Node),
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
    %% ct:pal("envs: ~p", [application:get_all_env(vonnegut)]),
    application:start(shackle),
    ok = vg_client_pool:start(),
    Config.

end_per_testcase(_TestCase, Config) ->
    application:unload(vonnegut),
    ok = vg_client_pool:stop(),
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
       %% start_predefined
       %% start_dynamic
       %% terminate_dynamic
       %% kill_random
      ]}

    ].

all() ->
    [
     {group, init} %,
     %% {group, operations}
    ].

bootstrap(Config) ->
    %% just create topics when written to for now
    %% do(vg, create_topic, [<<"foo">>]),
    R = vg_client:produce(<<"foo">>, <<"bar">>),
    R1 = vg_client:fetch(<<"foo">>),
    ct:pal("r ~p ~p", [R, R1]),
    timer:sleep(200),
    ?assertMatch(foo, R),
    Config.

%% start_predefined(Config) ->
%%     Config.

%%% utils

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

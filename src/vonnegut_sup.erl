%%%-------------------------------------------------------------------
%% @doc vonnegut top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vonnegut_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_cluster_mgr/2,
         start_acceptor_pool/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_cluster_mgr(Name, Nodes) ->
    {ok, [LogDir]} = application:get_env(vonnegut, log_dirs),
    ChildSpec = #{id      => vg_cluster_mgr,
                  start   => {vg_cluster_mgr, start_link, [Name, Nodes, LogDir]},
                  restart => permanent,
                  type    => supervisor},
    supervisor:start_child(?SERVER, ChildSpec).

start_acceptor_pool(Role) ->
    ChildSpec = #{id      => vg_pool_sup,
                  start   => {vg_pool_sup, start_link, [Role]},
                  restart => permanent,
                  type    => supervisor},
    supervisor:start_child(?SERVER, ChildSpec).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    Port = application:get_env(vonnegut, http_port, 8000),
    ElliChild = {vonnegut_http, {elli, start_link, [[{callback, elli_middleware},
                                                     {callback_args, [{mods, [{elli_prometheus, []},
                                                                              {vg_elli_handler, []}]}]},
                                                     {port, Port}]]},
                permanent, 5000, worker, dynamic},

    case application:get_env(vonnegut, chain, []) of
        [] ->
            {ok, {{one_for_one, 10, 30}, []}};
        _ ->
            ChainState = {vg_chain_state, {vg_chain_state, start_link, []},
                          permanent, 20000, worker, [vg_chain_state]},
            TopicsSup = {vg_topics_sup, {vg_topics_sup, start_link, []},
                         permanent, 20000, supervisor, [vg_topics_sup]},

            {ok, {{one_for_one, 10, 30}, [ElliChild, TopicsSup, ChainState]}}
    end.

%%====================================================================
%% Internal functions
%%====================================================================

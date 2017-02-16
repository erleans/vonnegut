%%%-------------------------------------------------------------------
%% @doc vonnegut top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vonnegut_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_cluster_mgr/2]).

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

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    case application:get_env(vonnegut, chain, []) of
        [] ->
            {ok, {{one_for_one, 10, 30}, []}};
        _ ->
            ChainState = {vg_chain_state, {vg_chain_state, start_link, []},
                          permanent, 20000, worker, [vg_chain_state]},
            TopicsSup = {vg_topics_sup, {vg_topics_sup, start_link, []},
                         permanent, 20000, supervisor, [vg_topics_sup]},
            PoolSup = {vg_pool_sup, {vg_pool_sup, start_link, []},
                       permanent, 20000, supervisor, [vg_pool_sup]},

            {ok, {{one_for_one, 10, 30}, [TopicsSup, ChainState, PoolSup]}}
    end.

%%====================================================================
%% Internal functions
%%====================================================================

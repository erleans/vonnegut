%%%-------------------------------------------------------------------
%% @doc vonnegut top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vonnegut_sup).

-compile([warn_missing_spec_all, warn_untyped_record]).

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

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec start_cluster_mgr(vg_chain_state:chain_name(), [vg_chain_state:chain_node()]) -> supervisor:startchild_ret().
start_cluster_mgr(Name, Nodes) ->
    {ok, [LogDir]} = application:get_env(vonnegut, log_dirs),
    ChildSpec = #{id      => vg_cluster_mgr,
                  start   => {vg_cluster_mgr, start_link, [Name, Nodes, LogDir]},
                  restart => permanent,
                  type    => supervisor},
    supervisor:start_child(?SERVER, ChildSpec).

-spec start_acceptor_pool(vg_chain_state:role()) -> supervisor:startchild_ret().
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
-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
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

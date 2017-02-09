%%%-------------------------------------------------------------------
%% @doc vonnegut top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vonnegut_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_cluster_mgr/0,
         create_topic/1,
         create_topic/2,
         create_topic/3]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

create_topic(Topic) ->
    create_topic(Topic, [0]).

create_topic(Topic, Partitions) ->
    create_topic(local, Topic, Partitions).

create_topic(Server0, Topic, Partitions) ->
    Server =
        case Server0 of
            local -> ?SERVER;
            _ -> {?SERVER, Server0}
        end,
    lager:info("at=create_topic node=~p topic=~p partitions=~p target=~p",
               [node(), Topic, Partitions, Server0]),
    supervisor:start_child(Server, topic_childspec(Topic, Partitions)).

start_cluster_mgr() ->
    {ok, [LogDir]} = application:get_env(vonnegut, log_dirs),
    ChildSpec = #{id      => vg_cluster_mgr,
                  start   => {vg_cluster_mgr, start_link, [LogDir]},
                  restart => permanent,
                  type    => supervisor},
    supervisor:start_child(?SERVER, ChildSpec).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    {ok, [LogDir]} = application:get_env(vonnegut, log_dirs),
    Topics = topic_childspecs(LogDir ++ atom_to_list(node())),

    ChainState = {vg_chain_state, {vg_chain_state, start_link, []},
                  permanent, 20000, worker, [vg_chain_state]},
    PoolSup = {vg_pool_sup, {vg_pool_sup, start_link, []},
               permanent, 20000, supervisor, [vg_pool_sup]},

    {ok, {{one_for_one, 10, 30}, [ChainState, PoolSup | Topics]}}.

%%====================================================================
%% Internal functions
%%====================================================================

topic_childspecs(DataDir) ->
    TopicPartitions = filelib:wildcard(filename:join(DataDir, "*")),
    TPDict = lists:foldl(fun(TP, Acc) ->
                                 case string:tokens(filename:basename(TP), "-") of
                                     [_] ->
                                         Acc;
                                     L ->
                                         [P | TopicR] = lists:reverse(L),
                                         T = string:join(lists:reverse(TopicR), "-"),
                                         dict:append_list(list_to_binary(T), [list_to_integer(P)], Acc)
                                 end
                         end, dict:new(), TopicPartitions),

    [topic_childspec(Topic, Partitions) || {Topic, Partitions} <- dict:to_list(TPDict)].

topic_childspec(Topic, Partitions) ->
    #{id      => Topic,
      start   => {vg_topic_sup, start_link, [Topic, Partitions]},
      restart => permanent,
      type    => supervisor}.

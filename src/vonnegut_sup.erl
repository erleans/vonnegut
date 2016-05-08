%%%-------------------------------------------------------------------
%% @doc vonnegut top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vonnegut_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         create_topic/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

create_topic(Topic) ->
    supervisor:start_child(?SERVER, topic_childspec(Topic, [0])).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    DataDir = "./data",
    topic_childspecs(DataDir),

    ClusterWatcher = {vonnegut_cluster, {vonnegut_cluster, start_link, []},
                      permanent, 20000, worker, [vonnegut_cluster]},

    {ok, {{one_for_one, 10, 30}, [ClusterWatcher]}}.

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
      restart => transient,
      type    => supervisor}.

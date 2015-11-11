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
    ChildSpecs = topic_childspecs(DataDir),
    {ok, {{one_for_one, 0, 1}, ChildSpecs}}.

%%====================================================================
%% Internal functions
%%====================================================================

topic_childspecs(DataDir) ->
    TopicPartitions = filelib:wildcard(filename:join(DataDir, "*")),
    TPDict = lists:foldl(fun(TP, Acc) ->
                                 case string:tokens(filename:basename(TP), "-") of
                                     [T, P] ->
                                         dict:append_list(list_to_binary(T), [list_to_integer(P)], Acc);
                                     _ ->
                                         Acc
                                 end
                         end, dict:new(), TopicPartitions),

    [topic_childspec(Topic, Partitions) || {Topic, Partitions} <- dict:to_list(TPDict)].

topic_childspec(Topic, Partitions) ->
    #{id      => Topic,
      start   => {vg_topic_sup, start_link, [Topic, Partitions]},
      restart => transient,
      type    => supervisor}.

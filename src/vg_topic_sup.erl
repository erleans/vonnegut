%%%-------------------------------------------------------------------
%% @doc vonnegut top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vg_topic_sup).

-behaviour(supervisor).

%% API
-export([start_link/2]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link(Topic, Partitions) ->
    supervisor:start_link(?MODULE, [Topic, Partitions]).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([Topic, Partitions]) ->
    ChildSpecs = lists:flatten([child_specs(Topic, Partition) || Partition <- Partitions]),
    {ok, {{one_for_one, 0, 1}, ChildSpecs}}.

%%====================================================================
%% Internal functions
%%====================================================================

child_specs(Topic, Partition) ->
    %% wait for the chain to be active?
    Next = vg_chain_state:next(),
    [#{id      => {active, Topic, Partition},
       start   => {vg_active_segment, start_link, [Topic, Partition, Next]},
       restart => permanent,
       type    => worker},
     #{id      => {mgr, Topic, Partition},
       start   => {vg_topic_mgr, start_link, [Topic, Partition, Next]},
       restart => permanent,
       type    => worker}
     | case application:get_env(vonnegut, log_cleaner, true) of
           true ->
               [#{id      => {cleaner, Topic, Partition},
                  start   => {vg_cleaner, start_link, [Topic, Partition]},
                  restart => permanent,
                  type    => worker}];
           false ->
               []
       end].

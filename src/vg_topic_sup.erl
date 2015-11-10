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
    supervisor:start_link({via, gproc, {n,l,Topic}}, ?MODULE, [Topic, Partitions]).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([Topic, Partitions]) ->
    ChildSpecs = [#{id      => {Topic, Partition},
                    start   => {vg_log, start_link, [Topic, Partition]},
                    restart => permanent,
                    type    => worker} || Partition <- Partitions],

    {ok, {{one_for_one, 0, 1}, ChildSpecs}}.

%%====================================================================
%% Internal functions
%%====================================================================

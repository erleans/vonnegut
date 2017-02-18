%%%-------------------------------------------------------------------
%% @doc vonnegut topics supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vg_topics_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_child/1,
         start_child/2,
         start_child/3]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_child(Topic) ->
    start_child(Topic, [0]).

start_child(Topic, Partitions) ->
    start_child(local, Topic, Partitions).

start_child(Server0, Topic, Partitions) ->
    Server =
        case Server0 of
            local -> ?SERVER;
            _ -> {?SERVER, Server0}
        end,
    lager:info("at=create_topic node=~p topic=~p partitions=~p target=~p",
               [node(), Topic, Partitions, Server0]),
    supervisor:start_child(Server, [Topic, Partitions]).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    SupFlags = #{strategy => simple_one_for_one,
                intensity => 0,
                period => 1},
    ChildSpecs = [#{id => vg_topic_sup,
                    start => {vg_topic_sup, start_link, []},
                    restart => permanent,
                    type => supervisor,
                    shutdown => 5000}],
    {ok, {SupFlags, ChildSpecs}}.

%%====================================================================
%% Internal functions
%%====================================================================

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

    ChildSpecs = lists:flatten([child_specs(Topic, Partition) || Partition <- Partitions]),

    {ok, {{one_for_one, 0, 1}, ChildSpecs}}.

%%====================================================================
%% Internal functions
%%====================================================================

child_specs(Topic, Partition) ->
    Segments = segments(Topic, Partition),
    [#{id      => {Topic, Partition},
       start   => {vg_log, start_link, [Topic, Partition]},
       restart => permanent,
       type    => worker} | Segments].

-spec segments(Topic, Partition) -> [] when
      Topic     :: binary(),
      Partition :: integer().
segments(Topic, Partition) ->
    {ok, [LogDir]} = application:get_env(vonnegut, log_dirs),
    TopicPartitionDir = filename:join(LogDir, [binary_to_list(Topic), "-", integer_to_list(Partition)]),
    LogSegments = filelib:wildcard(filename:join(TopicPartitionDir, "*.log")),
    [#{id      => {Topic, Partition, list_to_integer(filename:basename(LogSegment, ".log"))},
       start   => {vg_log_segment, start_link, [Topic, Partition, list_to_integer(filename:basename(LogSegment, ".log"))]},
       restart => permanent,
       type    => worker} || LogSegment <- LogSegments].

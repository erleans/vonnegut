%%%-------------------------------------------------------------------
%% @doc vonnegut top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vg_topic_sup).

-behaviour(supervisor).

%% API
-export([start_link/2,
         start_segment/3]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link(Topic, Partitions) ->
    supervisor:start_link({via, gproc, {n,l,Topic}}, ?MODULE, [Topic, Partitions]).

-spec start_segment(Topic, Partition, SegmentId) -> supervisor:startchild_ret() when
      Topic     :: binary(),
      Partition :: integer(),
      SegmentId :: integer().
start_segment(Topic, Partition, SegmentId) ->
    supervisor:start_child({via, gproc, {n,l,Topic}}, log_segment_childspec(Topic, Partition, SegmentId)).

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

    %% Must start segments before partition proc so it can find which segment is active
    [#{id      => {Topic, Partition},
       start   => {vg_log, start_link, [Topic, Partition, last]},
       restart => permanent,
       type    => worker},
     #{id      => {cleaner, Topic, Partition},
       start   => {vg_cleaner, start_link, [Topic, Partition]},
       restart => permanent,
       type    => worker} | Segments].

-spec segments(Topic, Partition) -> [] when
      Topic     :: binary(),
      Partition :: integer().
segments(Topic, Partition) ->
    TopicDir = vg_utils:topic_dir(Topic, Partition),
    case filelib:wildcard(filename:join(TopicDir, "*.log")) of
        [] ->
            [log_segment_childspec(Topic, Partition, 0)];
        LogSegments ->
            [log_segment_childspec(Topic, Partition, list_to_integer(filename:basename(LogSegment, ".log")))
            || LogSegment <- LogSegments]
    end.

log_segment_childspec(Topic, Partition, LogSegment) ->
    #{id      => {Topic, Partition, LogSegment},
      start   => {vg_log_segment, start_link, [Topic, Partition, LogSegment]},
      restart => transient,
      type    => worker}.

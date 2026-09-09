%%%-------------------------------------------------------------------
%% @doc vonnegut topics supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(vg_topics_sup).

-compile([warn_missing_spec_all, warn_untyped_record]).

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_child/1,
         start_child/2,
         start_child/3,
         start_child/4,
         list_topics/1,
         stop_child/3]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec start_child(vg:topic()) -> {ok, pid()} | {error, term()}.
start_child(Topic) ->
    start_child(Topic, [0]).

-spec start_child(vg:topic(), [vg:partition()]) -> {ok, pid()} | {error, term()}.
start_child(Topic, Partitions) ->
    start_child(local, Topic, Partitions).

-spec start_child(node(), vg:topic(), [vg:partition()]) -> {ok, pid()} | {error, term()}.
start_child(Server0, Topic, Partitions) ->
    %% since it's crucial to start remote children, block for a while
    start_child(Server0, Topic, Partitions, 300).

-spec start_child(node(), vg:topic(), [vg:partition()], non_neg_integer()) -> {ok, pid()} | {error, term()}.
start_child(_, _, _, 0) ->
    {error, remote_node_down};
start_child(Server0, Topic, Partitions, Retries) ->
    Server = case Server0 of
                 local -> ?SERVER;
                 _ -> {?SERVER, Server0}
             end,
    logger:info("at=create_topic node=~p topic=~p partitions=~p target=~p",
               [node(), Topic, Partitions, Server0]),
    prometheus_gauge:inc(active_topics),
    try
        case supervisor:start_child(Server, [Topic, Partitions]) of
            {ok, Pid} when is_pid(Pid) ->
                {ok, Pid};
            {error, {already_started, Pid}} when is_pid(Pid) ->
                {ok, Pid};
            {error, {shutdown, {failed_to_start_child, _, Reason}}} ->
                {error, Reason}
        end
    catch _C:_E->
            logger:info("~p : ~p", [_C,_E]),
            timer:sleep(100),
            start_child(Server0, Topic, Partitions, Retries - 1)
    end.

-spec stop_child(node(), vg:topic(), [vg:partition()]) -> [ok | {error, term()}].
stop_child(Server, Topic, Partitions) ->
    %% get a list of topic_sup supervisors
    Topics = supervisor:which_children({?MODULE, Server}),
    Topics1 = [{Pid, supervisor:which_children(Pid)}
               || {_, Pid, _, _} <- Topics],
    Res =
        [[case Topic =:= Top andalso lists:member(Part, Partitions) of
              true ->
                  supervisor:terminate_child({?MODULE, Server}, Pid);
              _ ->
                  ok
          end
          || {{active, Top, Part}, _, _, _} <- Children]
         || {Pid, Children} <- Topics1],
    lists:usort(lists:flatten(Res)).

-spec list_topics(node()) -> [{vg:topic(), vg:partition()}].
list_topics(Server0) ->
    Server = case Server0 of
                 local -> ?SERVER;
                 _ -> {?SERVER, Server0}
             end,
    Topics = supervisor:which_children(Server),
    [{Topic, Partition} || {{active, Topic, Partition}, _, _, _} <-
               lists:flatten([supervisor:which_children(Pid)
                              || {_, Pid, _, _} <- Topics]),
                         is_binary(Topic), is_integer(Partition)].

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
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


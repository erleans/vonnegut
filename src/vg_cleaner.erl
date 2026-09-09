-module(vg_cleaner).

-compile([warn_missing_spec_all, warn_untyped_record]).

-behaviour(gen_server).

-export([start_link/2,
         run_cleaner/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {topic_dir          :: file:filename_all(),
                topic              :: binary(),
                partition          :: integer(),
                retention_check_ms :: integer(),
                retention_seconds  :: integer(),
                t_ref              :: timer:tref()}).

-define(SERVER(Topic, Partition), {via, gproc, {n, l, {vg_cleaner, Topic, Partition}}}).

-spec start_link(vg:topic(), vg:partition()) -> gen_server:start_ret().
start_link(Topic, Partition) ->
    gen_server:start_link(?SERVER(Topic, Partition),
                          ?MODULE, [Topic, Partition], []).

-spec run_cleaner(vg:topic(), vg:partition()) -> ok.
run_cleaner(Topic, Partition) ->
    gen_server:call(?SERVER(Topic, Partition), run_cleaner).

-spec init([vg:topic() | vg:partition()]) -> {ok, #state{}}.
init([Topic, Partition]) when is_binary(Topic), is_integer(Partition) ->
    {ok, RetentionCheckMin} = application:get_env(vonnegut, log_retention_check_interval),
    {ok, RetentionMinutes} = application:get_env(vonnegut, log_retention_minutes),
    RetentionCheckMs = round(timer:minutes(RetentionCheckMin)),
    RetentionSeconds = RetentionMinutes * 60,

    TopicDir = vg_utils:topic_dir(Topic, Partition),
    {ok, TRef} = timer:send_after(RetentionCheckMs, run_cleaner),
    {ok, #state{topic_dir=TopicDir,
                topic=Topic,
                partition=Partition,
                retention_check_ms=RetentionCheckMs,
                retention_seconds=RetentionSeconds,
                t_ref=TRef}}.

-spec handle_call(run_cleaner, gen_server:from(), #state{}) -> {reply, ok, #state{}}.
handle_call(run_cleaner, _From, State=#state{topic_dir=TopicDir,
                                             topic=Topic,
                                             partition=Partition,
                                             retention_check_ms=RetentionCheckMs,
                                             retention_seconds=RetentionSeconds,
                                             t_ref=TRef}) ->
    timer:cancel(TRef),
    run_cleaner_(TopicDir, Topic, Partition, RetentionSeconds),
    {ok, TRef1} = timer:send_after(RetentionCheckMs, run_cleaner),
    {reply, ok, State#state{t_ref=TRef1}}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(_, State) ->
    {noreply, State}.

-spec handle_info(run_cleaner, #state{}) -> {noreply, #state{}}.
handle_info(run_cleaner, State=#state{topic_dir=TopicDir,
                                      topic=Topic,
                                      partition=Partition,
                                      retention_check_ms=RetentionCheckMs,
                                      retention_seconds=RetentionSeconds}) ->
    run_cleaner_(TopicDir, Topic, Partition, RetentionSeconds),
    {ok, TRef} = timer:send_after(RetentionCheckMs, run_cleaner),
    {noreply, State#state{t_ref=TRef}}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), #state{}, term()) -> {ok, #state{}}.
code_change(_, State, _) ->
    {ok, State}.

%% Internal functions

-spec run_cleaner_(file:filename_all(), vg:topic(), vg:partition(), non_neg_integer()) -> ok.
run_cleaner_(TopicDir, Topic, Partition, RetentionSeconds) ->
    Segments = filelib:wildcard(filename:join(ec_cnv:to_list(TopicDir), "*.log")),
    Now = calendar:datetime_to_gregorian_seconds(calendar:universal_time()),
    lists:foreach(fun(Segment) ->
                          LastModified = {{_, _, _}, {_, _, _}} = filelib:last_modified(Segment),
                          [LastUniversal | _] = calendar:local_time_to_universal_time_dst(LastModified),
                          Diff =  Now - calendar:datetime_to_gregorian_seconds(LastUniversal),
                          if
                              Diff >= RetentionSeconds ->
                                  SegmentId = filename:basename(Segment, ".log"),
                                  logger:info("at=delete topic=~s partition=~p segment=~s", [Topic, Partition, SegmentId]),
                                  RootName = filename:rootname(Segment),
                                  ok = file:delete(RootName++".index"),
                                  ok = file:delete(Segment);
                              true ->
                                  ok
                          end
                  end, Segments).

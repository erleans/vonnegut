%% doesn't need to be constantly running along side the active segment.
%% TODO: turn into a one off proc that triggers when needed.
-module(vg_topic_mgr).

-compile([warn_missing_spec_all, warn_untyped_record]).

-behaviour(gen_server).

%% API
-export([
         start_link/3,
         delete_topic/2,
         regenerate_index/2
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state,
        {
          topic :: binary(),
          partition :: non_neg_integer(),
          next :: atom()
        }).

%%%===================================================================
%%% API
%%%===================================================================

-define(TOPIC_MGR(Topic, Partition), {via, gproc, {n, l, {mgr, Topic, Partition}}}).

-spec start_link(vg:topic(), vg:partition(), node()) -> {ok, pid()} | {error, term()}.
start_link(Topic, Partition, Next) ->
    case gen_server:start_link(?TOPIC_MGR(Topic, Partition), ?MODULE, [Topic, Partition, Next],
                               [{hibernate_after, timer:minutes(5)}]) of % hibernate after 5 minutes with no messages
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} when is_pid(Pid) ->
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

-spec delete_topic(vg:topic(), vg:partition()) -> ok | {error, term()}.
delete_topic(Topic, Partition) ->
    %% may need to start the topic if this fails?
    gen_server:call(?TOPIC_MGR(Topic, Partition), delete_topic, timer:seconds(45)).

-spec regenerate_index(vg:topic(), vg:partition()) -> ok.
regenerate_index(Topic, Partition) ->
    %% may need to start the topic if this fails?
    gen_server:call(?TOPIC_MGR(Topic, Partition), regenerate_index, timer:minutes(15)).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init([vg:topic() | vg:partition() | node()]) -> {ok, #state{}}.
init([Topic, Partition, Next]) when is_binary(Topic), is_integer(Partition), is_atom(Next) ->
    {ok, #state{topic = Topic,
                partition = Partition,
                next = Next}}.

-spec handle_call(term(), gen_server:from(), #state{}) -> {reply, ok, #state{}} | {noreply, #state{}}.
handle_call(delete_topic, _From, #state{topic = Topic, next = Next,
                                        partition = Partition} = State) ->
    %% halt the active segment
    logger:info("halting active segment"),
    halted = vg_active_segment:halt(Topic, Partition),
    %% delete the segments
    logger:info("deleting segments"),
    ok = vg_log_segments:delete_segments(Topic, Partition),
    %% remove HWM
    true = vg_topics:delete_hwm(Topic, Partition),
    %% clean the segments table
    vg_log_segments:cleanup_segments_table(Topic, Partition),
    %% delete the next
    case Next of
        tail -> ok;
        _ ->
            logger:info("propagating delete"),
            ok = vg_client:delete_topic(next_brick, Topic)
    end,
    {reply, ok, State};
%% note that this needs to be done per node, we don't automatically
%% propagate it
handle_call(regenerate_index, _From, #state{topic = Topic,
                                            partition = Partition} = State) ->
    %% tell active_segment to stop writing indexes
    ok = vg_active_segment:stop_indexing(Topic, Partition),
    %% delete all index files
    ok = vg_log_segments:delete_indexes(Topic, Partition),
    %% fold over segments and restore indexes
    ok = vg_log_segments:regenerate_indexes(Topic, Partition),
    %% tell active_segment to resume writing indexes
    ok = vg_active_segment:resume_indexing(Topic, Partition),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    logger:warning("unexpected call ~p from ~p", [_Request, _From]),
    {noreply, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
handle_cast(_Msg, State) ->
    logger:warning("unexpected cast ~p", [_Msg]),
    {noreply, State}.

-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(_Info, State) ->
    logger:warning("unexpected message ~p", [_Info]),
    {noreply, State}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), #state{}, term()) -> {ok, #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-module(vg_topic_mgr).

-behaviour(gen_server).

%% API
-export([
         start_link/3,
         delete_topic/2
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

start_link(Topic, Partition, Next) ->
    case gen_server:start_link(?TOPIC_MGR(Topic, Partition), ?MODULE, [Topic, Partition, Next],
                               [{hibernate_after, timer:minutes(5)}]) of % hibernate after 5 minutes with no messages
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

delete_topic(Topic, Partition) ->
    %% may need to start the topic if this fails?
    gen_server:call(?TOPIC_MGR(Topic, Partition), delete_topic, 30000).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Topic, Partition, Next]) ->
    {ok, #state{topic = Topic,
                partition = Partition,
                next = Next}}.

handle_call(delete_topic, _From, #state{topic = Topic, next = Next,
                                        partition = Partition} = State) ->
    %% halt the active segment
    lager:info("halting active segment"),
    ok = vg_active_segment:halt(Topic, Partition),
    %% delete the segments
    lager:info("deleting segments"),
    ok = vg_log_segments:delete_segments(Topic, Partition),
    %% remove HWM
    true = vg_topics:delete_hwm(Topic, Partition),
    %% clean the segments table
    vg_log_segments:cleanup_segments_table(Topic, Partition),
    %% delete the next
    case Next of
        tail -> ok;
        _ ->
            lager:info("propagating delete"),
            ok = vg_client:delete_topic(next_brick, Topic)
    end,
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    lager:warning("unexpected call ~p from ~p", [_Request, _From]),
    {noreply, State}.

handle_cast(_Msg, State) ->
    lager:warning("unexpected cast ~p", [_Msg]),
    {noreply, State}.

handle_info(_Info, State) ->
    lager:warning("unexpected message ~p", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

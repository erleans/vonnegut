-module(vg_statem).

-include_lib("proper/include/proper.hrl").

-export([command/1, initial_state/0, next_state/3,
         precondition/2, postcondition/3]).

%% sigh
-export([restart_server/0]).

-record(state,
        {
          topics = #{}
        }).

command(_S = #state{topics = Topics}) ->
    %% replace with proper oneof when maps are supported
    {Topic, Info} = one_of(Topics),
    Index = hwm(Info) + 1,
    frequency(
      [%% to tickle close and restart validation bugs
       {2,   {call, ?MODULE, restart_server, []}},
       {2,    {call, vg, ensure_topic, [?LET(A, atom(), atom_to_binary(A, utf8))]}},
       %% write more than is common in typical workloads in order to
       %% trigger more wraps
       {40,   {call, vg, write,
               %%[Topic, message(Topic, Index, binary())]}},
               [Topic, 0, message(Topic, Index, <<>>)]}},
       {10,   {call, vg, write,
               %%[Topic, message(Topic, Index, binary())]}},
               [Topic, 0, ?LET(I, integer(2, 15),
                               [message(Topic, Index + N, <<>>)
                                || N <- lists:seq(0, I)])]}},
       {40, {call, vg, fetch, [Topic, 0, -1, integer(1, 5)]}},
       {100, {call, vg, fetch, [Topic, integer(0, Index - 1)]}}
      ]).

%% Initial model value at system start. Should be deterministic.
initial_state() ->
    application:load(vonnegut),
    %% set this once per run
    application:set_env(vonnegut, log_dirs, [filename:join("properdata", integer_to_list(erlang:system_time()))]),

    _ = application:stop(vonnegut),
    {ok, _} = application:ensure_all_started(vonnegut),
    timer:sleep(500),
    ok = vg:create_topic(<<"seed">>),
    {ok, 0} = vg:write(<<"seed">>, 0, message(<<"seed">>, 0, <<>>)),
    #state{topics = #{<<"seed">> => 0}}.

%% Picks whether a command should be valid under the current state.
precondition(#state{topics = T}, {call, _Mod, Fun, _Args}) when T == #{}
                                                                 andalso (Fun == fetch orelse
                                                                          Fun == produce) ->
    false;
precondition(#state{}, _C = {call, _Mod, _Fun, _Args}) ->
    %lager:info("calling ~p", [_C]),
    true.

%% Given the state `State' *prior* to the call `{call, Mod, Fun, Args}',
%% determine whether the result `Res' (coming from the actual system)
%% makes sense.
postcondition(#state{topics = Topics}, {call, vg_client, fetch, [Topic, Index]}, Res) ->
    HWM = maps:get(Topic, Topics, undefined),
    {ok, #{Topic := #{0 := #{record_set := RecordSet}}}} = Res,
    %% potentially validate what we're seeing here, also this will stop being true once more bytes start being returned
    length(RecordSet) =:= (HWM - Index) + 1;
postcondition(#state{topics = Topics}, {call, vg_client, produce, [Topic, Message]}, Res) ->
    lager:info("check ~p ~p ~p", [Topic, Res, Topics]),
    HWM = maps:get(Topic, Topics),
    {ok, Offset} = Res,
    %% potentially validate what we're seeing here
    Added =
        case Message of
            L when is_list(L) ->
                length(L);
            _ -> 1
        end,
    Offset =:= HWM + Added;
postcondition(_State, {call, _Mod, _Fun, _Args}, _Res) ->
    true.

%% Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(State=#state{topics = Topics}, {ok, Offset}, {call, vg_client, produce, [Topic, _Message]}) ->
    NewTopics = maps:put(Topic, Offset, Topics),
    State#state{topics = NewTopics};
next_state(State=#state{topics = Topics}, {ok, _}, {call, vg_client, ensure_topic, [Topic]}) ->
    NewTopics = maps:put(Topic, 0, Topics),
    State#state{topics = NewTopics};
next_state(State, {error, _}, {call, vg_client, ensure_topic, [_Topic]}) ->
    %% i guess just ignore invalid topic names?
    State;
next_state(State, _Res, {call, _Mod, _Fun, _Args}) ->
    NewState = State,
    NewState.


%%%%%%%%%

restart_server() ->
    ok = application:stop(vonnegut),
    ok = application:start(vonnegut),
    timer:sleep(500),
    ok.

message(Topic, Index, Gen) ->
    Reps = rand:uniform(15) + 5,
    iolist_to_binary(
      [<<Index:64/native,
         Reps:16/native>>,
       lists:duplicate(Reps,
                       <<Topic/binary,
                         (integer_to_binary(Index))/binary>>),
       <<(byte_size(Gen)):32/native, Gen/binary>>]).

%% trivial but opaque for interface reasons
hwm(HWM) ->
    HWM.

one_of(Empty) when Empty =:= #{} ->
    %% bad feeling here, buttttt
    {<<>>, 0};
one_of(Map) ->
    Sz = maps:size(Map),
    Index = rand:uniform(Sz),
    lists:nth(Index, maps:to_list(Map)).

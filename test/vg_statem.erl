-module(vg_statem).

-include_lib("proper/include/proper.hrl").

-export([command/1, initial_state/0, next_state/3,
         precondition/2, postcondition/3]).

-record(state, {topics = #{}}).

-define(TOPICS, [<<"topic1">>, <<"topic2">>, <<"topic3">>]).

command(_State) ->
    frequency(
      [{1, {call, vg, write, [oneof(?TOPICS), binary()]}},
       {5, {call, vg, fetch, [oneof(?TOPICS)]}}
      ]).

%% Initial model value at system start. Should be deterministic.
initial_state() ->
    application:ensure_all_started(vonnegut),
    ok = vg:create_topic(<<"topic1">>),
    ok = vg:create_topic(<<"topic2">>),
    ok = vg:create_topic(<<"topic3">>),
    #state{topics = #{<<"topic1">> => vg:fetch(<<"topic1">>),
                      <<"topic2">> => vg:fetch(<<"topic2">>),
                      <<"topic3">> => vg:fetch(<<"topic3">>)}}.

%% Picks whether a command should be valid under the current state.
precondition(#state{}, {call, _Mod, _Fun, _Args}) ->
    true.

%% Given the state `State' *prior* to the call `{call, Mod, Fun, Args}',
%% determine whether the result `Res' (coming from the actual system)
%% makes sense.
postcondition(#state{topics=Topics}, {call, vg, get, [Topic]}, Res) ->
    Res =:= maps:get(Topic, Topics);
postcondition(_State, {call, _Mod, _Fun, _Args}, _Res) ->
    true.

%% Assuming the postcondition for a call was true, update the model
%% accordingly for the test to proceed.
next_state(State=#state{topics=Topics}, _Res, {call, vg, write, [Topic, Message]}) ->
    NewTopics = maps:update_with(Topic, fun(Msgs) -> [Message | Msgs] end, Topics),
    State#state{topics = NewTopics};
next_state(State, _Res, {call, _Mod, _Fun, _Args}) ->
    NewState = State,
    NewState.

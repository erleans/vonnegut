-module(vonnegut_cluster).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {ring}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    net_kernel:monitor_nodes(true, [nodedown_reason]),
    {ok, NodeList} = application:get_env(vonnegut, nodes),
    [net_kernel:connect(Node) || Node <- NodeList],
    schedule_tick(),
    Nodes = hash_ring:list_to_nodes([node() | nodes()]),
    Ring = hash_ring:make(Nodes),
    {ok, #state{ring=Ring}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(tick, State) ->
    State2 = tick(State),
    schedule_tick(),
    {noreply, State2};
handle_info({nodedown, Node, Reason}, State=#state{ring=Ring}) ->
    lager:info("nodedown=~p down=~p", [Node, Reason]),
    Ring1 = hash_ring:remove_node(Node, Ring),
    {noreply, State#state{ring=Ring1}};
handle_info({nodeup, Node, _}, State=#state{ring=Ring}) ->
    lager:info("nodeup=~p", [Node]),
    Ring1 = hash_ring:add_node(hash_ring_node:make(Node), Ring),
    {noreply, State#state{ring=Ring1}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
schedule_tick() ->
    erlang:send_after(1000, self(), tick).

tick(State) ->
    State.

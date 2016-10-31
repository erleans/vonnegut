-module(vg_topics).

-behaviour(gen_server).

%% API
-export([start_link/0,
         create_topic/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

create_topic(Topic) ->
    %%Chain = vg_chains:chain(Topic, 0),
    gen_server:call(?SERVER, {create_topic, Topic, 0}).

init([]) ->
    {ok, #state{}}.

handle_call({create_topic, Topic, _Partition}, _From, State) ->
    vonnegut_sup:create_topic(Topic),
    {reply, ok, State};
handle_call({create_topic, Topic, Partition, Chain}, From, State) ->
    spawn(fun() ->
              Nodes = [Node || {_, Node, _, _} <- Chain],
              gen_server:multi_call(Nodes, ?SERVER, {create_topic, Topic, Partition}),
              gen_server:reply(From, ok)
          end),
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

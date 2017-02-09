%%%-------------------------------------------------------------------
%%% @author Tristan Sloughter <>
%%% @copyright (C) 2017, Tristan Sloughter
%%% @doc
%%%
%%% @end
%%% Created :  9 Feb 2017 by Tristan Sloughter <>
%%%-------------------------------------------------------------------
-module(vg_cluster_mgr).

-behaviour(gen_server).

%% API
-export([start_link/1,
         get_map/0,
         create_topic/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("vg.hrl").

-type topic() :: binary().
-type chain_id() :: binary().
-type topics_map() :: #{topic() => chain_id()}.
-type chains_map() :: #{chain_id() => chain()}.

-export_types([topic/0,
               chain_id/0,
               topics_map/0,
               chains_map/0]).

-define(SERVER, ?MODULE).

-record(state, {topics = #{} :: maps:map(),
                chains = #{} :: maps:map(),
                epoch        :: integer()}).

start_link(DataDir) ->
    gen_server:start_link({global, ?SERVER}, ?MODULE, [DataDir], []).

-spec get_map() -> {Topics :: topics_map(), Chains :: chains_map(), Epoch :: integer()}.
get_map() ->
    gen_server:call({global, ?SERVER}, get_map).

-spec create_topic(Topic :: binary()) -> {ok, Chain :: binary()} | {error, exists}.
create_topic(Topic) ->
    gen_server:call({global, ?SERVER}, {create_topic, Topic}).

init([DataDir]) ->
    State = load_state(DataDir),
    {ok, State}.

handle_call(get_map, _From, State=#state{topics=Topics,
                                         chains=Chains,
                                         epoch=Epoch}) ->
    {reply, {Topics, Chains, Epoch}, State};
handle_call({create_topic, Topic}, _From, State=#state{topics=Topics,
                                                       chains=Chains,
                                                       epoch=Epoch}) ->
    case maps:get(Topic, Topics, not_found) of
        not_found ->
            Keys = maps:keys(Chains),
            Random = rand:uniform(length(Keys)),
            Chain = lists:nth(Random, Keys),

            %% needs to be done on the proper chain
            {ok, _} = vonnegut_sup:create_topic(Topic),

            Topics1 = maps:put(Topic, Chain, Topics),
            {reply, {ok, Chain}, State#state{topics=Topics1,
                                             epoch=Epoch+1}};
        Chain ->
            lager:info("attempting to create topic that already exists on chain=~p", [Chain]),
            {reply, {error, exists}, State}
    end.


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

load_state(_DataDir) ->
    Cluster = application:get_env(vonnegut, cluster, local),
    Chains = raw_to_chains(Cluster),
    ChainsMap = lists:foldl(fun(Chain=#chain{name=Name}, Acc) ->
                                maps:put(Name, Chain, Acc)
                            end, #{}, Chains),
    #state{topics = #{},
           chains = ChainsMap,
           epoch = 0}.

raw_to_chains({direct, Nodes}) ->
    ParsedNodes = [parse_name(Node) || Node <- Nodes],
    Chains = ordsets:from_list([Chain || {Chain, _, _, _} <- ParsedNodes]),
    [make_chain(Chain, ParsedNodes)
     || Chain <- Chains];
raw_to_chains(local) ->
    [#chain{name = <<"local">>,
            head = {"127.0.0.1", 5555},
            tail = {"127.0.0.1", 5555}}].

parse_name({Name0, Host, Port}) ->
    %% this especially is likely to cause problems if we allow
    %% arbitrary chain naming
    Name = atom_to_list(Name0),
    [Chain, Pos] = string:tokens(Name, "-"),
    {Chain, list_to_integer(Pos), Host, Port}.

make_chain(Chain, Nodes0) ->
    Nodes = [Node || Node = {C, _, _, _} <- Nodes0, C =:= Chain],
    Len = length(Nodes),
    {value, {_, _, Head, HeadPort}} = lists:keysearch(0, 2, Nodes),
    {value, {_, _, Tail, TailPort}} = lists:keysearch(Len - 1, 2, Nodes),
    #chain{name = list_to_binary(Chain),
           head = {Head, HeadPort},
           tail = {Tail, TailPort}}.

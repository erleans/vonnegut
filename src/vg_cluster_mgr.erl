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
-export([start_link/3,
         get_map/0,
         create_topic/1,
         ensure_topic/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("vg.hrl").

-type chain_id() :: binary().
-type topics_map() :: #{vg:topic() => chain_id()}.
-type chains_map() :: #{chain_id() => chain()}.

-export_types([topic/0,
               chain_id/0,
               topics_map/0,
               chains_map/0]).

-define(SERVER, ?MODULE).

-record(state, {topics = #{} :: maps:map(),
                chains = #{} :: maps:map(),
                epoch        :: integer()}).

-spec start_link(vg_chain_state:chain_name(), [vg_chain_state:chain_node()], file:filename_all()) -> {ok, pid()}.
start_link(ChainName, ChainNodes, DataDir) ->
    gen_server:start_link({global, ?SERVER}, ?MODULE, [ChainName, ChainNodes, DataDir], []).

%% add chain functionality needed

-spec get_map() -> {Topics :: topics_map(), Chains :: chains_map(), Epoch :: integer()}.
get_map() ->
    gen_server:call({global, ?SERVER}, get_map).

-spec create_topic(Topic :: binary()) -> {ok, Chain :: binary()} | {error, exists}.
create_topic(Topic) ->
    gen_server:call({global, ?SERVER}, {create_topic, Topic}).

-spec ensure_topic(ChainName :: vg_chain_state:chain_name(), Topic :: binary()) -> {error, chain_not_found} |
                                                                                   {error, topic_exists_other_chain} |
                                                                                   ok.
ensure_topic(ChainName, Topic) ->
    gen_server:call({global, ?SERVER}, {ensure_topic, ChainName, Topic}).

init([ChainName, ChainNodes, DataDir]) ->
    Chain = create_chain(ChainName, ChainNodes),
    State = load_state([Chain], DataDir),
    self() ! {ensure_topics, ChainName},
    {ok, State, 0}.

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

            %% start topic process on all nodes in the chain
            #chain{nodes=Nodes} = maps:get(Chain, Chains),
            [{ok, _} = vg_topics_sup:start_child(Node, Topic, [0]) || Node <- Nodes],

            Topics1 = maps:put(Topic, Chain, Topics),
            {reply, {ok, Chain}, State#state{topics=Topics1,
                                             epoch=Epoch+1}};
        Chain ->
            lager:info("attempting to create topic that already exists on chain=~p", [Chain]),
            {reply, {error, exists}, State}
    end;
handle_call({ensure_topic, ChainName, Topic}, _From, State) ->
    {Reply, State} = ensure_topic(ChainName, Topic),
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({ensure_topics, ChainName}, State) ->
    State1 = ensure_all_topics(ChainName, State),
    {noreply, State1}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_chain(Name, []) ->
    #chain{name  = Name,
           nodes = [node()],
           head  = {"127.0.0.1", 5555},
           tail  = {"127.0.0.1", 5555}};
create_chain(Name, Nodes) ->
    #chain{name  = Name,
           nodes = [nodename(Node) || Node <- Nodes],
           head  = head(Nodes),
           tail  = tail(Nodes)}.

nodename({Name, Host, _, _}) ->
    list_to_atom(atom_to_list(Name) ++ "@" ++ Host).

load_state(Chains, _DataDir) ->
    ChainsMap = lists:foldl(fun(Chain=#chain{name=Name}, Acc) ->
                                maps:put(Name, Chain, Acc)
                            end, #{}, Chains),
    #state{topics = #{},
           chains = ChainsMap,
           epoch = 0}.

head([{_, Host, _, ClientPort} | _]) ->
    {Host, ClientPort}.

tail(Nodes) ->
    head(lists:reverse(Nodes)).

ensure_all_topics(ChainName, State) ->
    Topics = vg_utils:topics_on_disk(),
    lists:foldl(fun({Topic, _}, StateAcc) ->
                    {_, StateAcc1} = ensure_topic(ChainName, Topic, StateAcc),
                    StateAcc1
                end, State, Topics).

ensure_topic(ChainName, Topic, State=#state{topics=Topics,
                                            chains=Chains,
                                            epoch=Epoch}) ->
    case maps:get(Topic, Topics, not_found) of
        not_found ->
            case maps:get(ChainName, Chains, not_found) of
                not_found ->
                    lager:error("at=ensure_topic error=chain_not_found chain=~s topic=~s", [ChainName, Topic]),
                    {{error, chain_not_found}, State};
                #chain{nodes=Nodes} ->
                    %% start topic process on all nodes in the chain
                    [{ok, _} = vg_topics_sup:start_child(Node, Topic, [0]) || Node <- Nodes],

                    Topics1 = maps:put(Topic, ChainName, Topics),
                    {ok, State#state{topics=Topics1,
                                              epoch=Epoch+1}}
            end;
        ChainName ->
            %% this is no problem
            lager:info("attempting to ensure topic that was already started on chain ~p", [ChainName]),
            {ok, State};
        OtherChain ->
            %% this is bad
            lager:error("attempted to ensure topic on chain ~p that already exists on a different chain ~p",
                        [ChainName, OtherChain]),
            {{error, topic_exists_other_chain}, State}
    end.

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
         ensure_topic/1]).

-export([
         create_topic/1,
         delete_topic/1,
         describe_topic/1,
         deactivate_topic/1,
         running_topics/0
        ]).

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
    gen_server:start_link({local, ?SERVER}, ?MODULE, [ChainName, ChainNodes, DataDir], []).

%% add chain functionality needed

-spec get_map() -> {Topics :: topics_map(), Chains :: chains_map(), Epoch :: integer()}.
get_map() ->
    HeadNode = vg_chain_state:head(),
    gen_server:call({?SERVER, HeadNode}, get_map).

-spec create_topic(Topic :: binary()) -> {ok, Chain :: binary()} | {error, exists}.
create_topic(Topic) ->
    HeadNode = vg_chain_state:head(),
    gen_server:call({?SERVER, HeadNode}, {create_topic, Topic}).

-spec ensure_topic(Topic :: binary()) -> {error, chain_not_found} |
                                         {error, topic_exists_other_chain} |
                                         {ok, chain_id()}.
ensure_topic(Topic) ->
    HeadNode = vg_chain_state:head(),
    gen_server:call({?SERVER, HeadNode}, {ensure_topic, Topic}).

delete_topic(Topic) ->
    HeadNode = vg_chain_state:head(),
    gen_server:call({?SERVER, HeadNode}, {delete_topic, Topic}, infinity).

describe_topic(Topic) ->
    HeadNode = vg_chain_state:head(),
    gen_server:call({?SERVER, HeadNode}, {describe_topic, Topic}).

deactivate_topic(Topic) ->
    HeadNode = vg_chain_state:head(),
    gen_server:call({?SERVER, HeadNode}, {deactivate_topic, Topic}).

running_topics() ->
    HeadNode = vg_chain_state:head(),
    gen_server:call({?SERVER, HeadNode}, running_topics).

%%%%%%%%%%%%%%%%%%%%%%%%

init([ChainName, ChainNodes, DataDir]) ->
    Chain = create_chain(ChainName, ChainNodes),
    State = load_state([Chain], DataDir),
    self() ! {ensure_topics, ChainName},
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
handle_call({ensure_topic, Topic}, _From, State=#state{topics=Topics,
                                                       chains=Chains,
                                                       epoch=Epoch}) ->
    case maps:get(Topic, Topics, not_found) of
        not_found ->
            Keys = maps:keys(Chains),
            Random = rand:uniform(length(Keys)),
            Chain = lists:nth(Random, Keys),

            %% start topic process on all nodes in the chain
            start_on_all_nodes(Topic, Chain, Chains),
            Topics1 = maps:put(Topic, Chain, Topics),
            {reply, {ok, Chain}, State#state{topics=Topics1,
                                             epoch=Epoch+1}};
        Chain ->
            start_on_all_nodes(Topic, Chain, Chains),
            {reply, {ok, Chain}, State}
    end;
handle_call({delete_topic, Topic}, _From, State=#state{topics=Topics,
                                                       chains=Chains}) ->
    %% have topic mgr delete the topic segments and directory
    %% deactivate the topic so that it can be recreated if desired
    {Reply, Topics1}  =
        case maps:get(Topic, Topics, not_found) of
            not_found -> {{error, not_found}, Topics};
            Chain ->
                Rep =
                    try
                        vg_topic_mgr:delete_topic(Topic, 0)  % eventually iterate partitions?
                    catch _:{noproc, _} ->
                            start_on_all_nodes(Topic, Chain, Chains),
                            vg_topic_mgr:delete_topic(Topic, 0)
                    end,
                stop_on_all_nodes(Topic, Chain, Chains),
                {Rep, maps:remove(Topic, Topics)}
        end,
    {reply, Reply, State#state{topics=Topics1}};
%% handle_call({describe_topic, Topic}, _From, State=#state{topics=Topics,
%%                                                          chains=Chains}) ->
%%     %% get the hwm
%%     %% get number of segments
%%     %% size on disk
%%     %% check if it's running?
%%     {reply, ok, State};
handle_call({deactivate_topic, Topic}, _From, State=#state{topics=Topics,
                                                           chains=Chains}) ->
    Ret =
        case maps:get(Topic, Topics, not_found) of
            not_found -> {error, not_found};
            Chain -> stop_on_all_nodes(Topic, Chain, Chains)
        end,
    {reply, Ret, State};
handle_call(running_topics, _From, State=#state{chains=_Chains}) ->
    %% TODO: need to do this for all chains?
    Ret = vg_topics_sup:list_topics(node()),
    {reply, Ret, State};
handle_call(_, _, State) ->
    {noreply, State}.

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

start_on_all_nodes(Topic, Chain, Chains) ->
    #chain{nodes=Nodes} = maps:get(Chain, Chains),
    [case vg_topics_sup:start_child(Node, Topic, [0]) of
         {ok, _} -> ok;
         {error,{already_started, _}} -> ok;
         {error, Reason} -> exit({error, Reason})
     end || Node <- Nodes].

stop_on_all_nodes(Topic, Chain, Chains) ->
    #chain{nodes=Nodes} = maps:get(Chain, Chains),
    %% usort here to remove useless oks
    lists:usort(
      [case vg_topics_sup:stop_child(Node, Topic, [0]) of
           [ok] -> ok;
           %% annotate and pass on the error for user analysis
           Other -> {Node, Topic, Other}
       end || Node <- Nodes]).

%% TODO: the topic space stuff MUST be fixed before multiple chains are supported
create_chain(Name, []) ->
    #chain{name  = Name,
           nodes = [node()],
           topics_start = start_space,
           topics_end = end_space,
           head  = {"127.0.0.1", 5588},
           tail  = {"127.0.0.1", 5588}};
create_chain(Name, Nodes) ->
    #chain{name  = Name,
           nodes = [nodename(Node) || Node <- Nodes],
           topics_start = start_space, % only valid for one chain
           topics_end = end_space,     % only valid for one chain
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
    lists:foldl(fun({Topic, _}, StateAcc=#state{topics=TopicsAcc,
                                                epoch=Epoch}) ->
                        TopicsAcc1 = maps:put(Topic, ChainName, TopicsAcc),
                        StateAcc#state{topics=TopicsAcc1,
                                       epoch=Epoch+1}
                end, State, Topics).


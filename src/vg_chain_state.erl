%%%-------------------------------------------------------------------
%% @doc Track the current state of the chain this node is a member of.
%%
%% @end
%%%-------------------------------------------------------------------
-module(vg_chain_state).

-behaviour(gen_statem).

-export([start_link/0,
         role/0,
         next/0]).

-export([init/1,
         active/3,
         inactive/3,
         callback_mode/0,
         terminate/3,
         code_change/4]).

-type chain_name() :: atom().
-type role() :: head | tail | middle | solo | undefined.
-type chain_node() :: {atom(), inet:ip_address() | inet:hostname(), inet:port_number(), inet:port_number()}.

-export_types([role/0,
               chain_node/0]).

-record(data, {
          name         :: chain_name(),
          role         :: role(),
          cluster_type :: vg_utils:cluster_type(),
          members      :: ordsets:ordset(),
          all_nodes    :: [chain_node()] | undefined,
          next_node    :: atom() | tail,
          replicas     :: integer()
         }).

-define(SERVER, ?MODULE).
-define(NODENAME, vonnegut).

start_link() ->
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

next() ->
    gen_statem:call(?SERVER, next_node).

role() ->
    gen_statem:call(?SERVER, role).

init([]) ->
    ChainName = vg_config:chain_name(),
    ClusterType = vg_config:cluster_type(),
    Replicas = vg_config:replicas(),
    {ok, inactive, #data{name=ChainName,
                         replicas=Replicas,
                         cluster_type=ClusterType}, [{state_timeout, 0, connect}]}.

inactive({call, From}, next_node, _Data) ->
    {keep_state_and_data, [{reply, From, undefined}]};
inactive({call, From}, role, _Data) ->
    {keep_state_and_data, [{reply, From, undefined}]};
inactive(state_timeout, connect, Data=#data{name=Name,
                                            replicas=Replicas,
                                            cluster_type=ClusterType}) ->
    {Members, AllNodes} = join(ClusterType),
    lager:info("cluster_type=~p members=~p all_nodes=~p", [ClusterType, Members, AllNodes]),
    case role(node(), Members, Replicas, ClusterType) of
        solo ->
            lager:info("at=chain_complete role=solo requested_size=1", []),
            lager:info("at=start_cluster_mgr role=solo"),
            vonnegut_sup:start_cluster_mgr(solo, []),
            {next_state, active, Data#data{members=Members,
                                           role=solo,
                                           all_nodes=[],
                                           next_node=tail}};
        undefined ->
            {keep_state, Data#data{members=Members,
                                   role=undefined}, [{state_timeout, 1000, connect}]};
        Role ->
            lager:info("at=chain_join role=~s members=~p", [Role, Members]),
            case length(Members) of
                Size when Size >= Replicas ->
                    case Role of
                        head ->
                            lager:info("at=start_cluster_mgr role=head"),
                            %% if cluster mgr isn't running, start it
                            %% otherwise, add this chain to the cluster mgr
                            %% and all our topics
                            vonnegut_sup:start_cluster_mgr(Name, AllNodes);
                        _ ->
                            ok
                    end,

                    %% monitor next link in the chain
                    NextNode = next_node(Role, node(), Members),
                    Self = self(),
                    vg_peer_service:on_down(NextNode, fun() -> Self ! {next_node_down, NextNode} end),

                    lager:info("at=chain_complete requested_size=~p", [Replicas]),
                    {next_state, active, Data#data{members=Members,
                                                   all_nodes=AllNodes,
                                                   role=Role,
                                                   next_node=NextNode}};
                Size ->
                    lager:info("at=chain_incomplete requested_size=~p current_size=~p", [Replicas, Size]),
                    {keep_state, Data#data{members=Members,
                                           role=Role}, [{state_timeout, 1000, connect}]}
            end
    end;
inactive(info, {next_node_down, NextNode}, _Data) ->
    lager:info("state=inactive next_node_down=~p", [NextNode]),
    {keep_state_and_data, [{state_timeout, 0, connect}]}.

active({call, From}, next_node, #data{next_node=NextNode}) ->
    {keep_state_and_data, [{reply, From, NextNode}]};
active({call, From}, role, #data{role=Role}) ->
    {keep_state_and_data, [{reply, From, Role}]};
active(info, {next_node_down, NextNode}, Data) ->
    lager:info("state=active next_node_down=~p", [NextNode]),
    {next_state, inactive, Data, 0}.

callback_mode() ->
    state_functions.

terminate(_Reason, _State, _Data) ->
    ok.

code_change(_, _OldState, Data, _) ->
    {ok, Data}.

%% Internal functions

%% assume we expect to find at least 1 node if using srv discovery
role(_Node, _, 1, _) ->
    head;
role(_Node, [], _, {srv, _}) ->
    undefined;
role(Node, [Node], _, {srv, _}) ->
    undefined;
role(_Node, [], _, local) ->
    solo;
role(Node, [Node], _, local) ->
    solo;
role(Node, [Node | _], _, _) ->
    head;
role(Node, Nodes, _, _) ->
    case lists:reverse(Nodes) of
        [Node | _] ->
            tail;
        _ ->
            middle
    end.

next_node(tail, _, _) ->
    tail;
next_node(head, _, [_, Next | _]) ->
    Next;
next_node(_, Node, Nodes) ->
    find_next(Node, Nodes).

-spec find_next(Node :: atom(), Nodes :: ordsets:ordset()) -> atom().
find_next(Node, Nodes) ->
    try
        %% set the accumulator when the node we are looking
        %% for the next of is found and throw to return
        %% the first element encountered after the acc is set
        ordsets:fold(fun(N, none) when Node =:= N ->
                         N;
                        (_, none) ->
                         none;
                        (N, _) ->
                         throw(N)
                     end, none, Nodes)
    catch
        throw:N ->
            N
    end.

join(ClusterType) ->
    AllNodes = lookup(ClusterType),
    ordsets:fold(fun({Name, Host, PartisanPort, _ClientPort}, _) ->
                     vg_peer_service:join({Name, Host, PartisanPort})
                 end, ok, AllNodes),
    {ok, Members} = vg_peer_service:members(),
    {lists:usort(Members), AllNodes}.

%% leave() ->
%%     vg_peer_service:leave([]).

%%

lookup(local) ->
    ordsets:new();
lookup(none) ->
    ordsets:new();
lookup({direct, Nodes}) ->
    ordsets:from_list(Nodes);
lookup({srv, DiscoveryDomain}) ->
    lists:foldl(fun({_, _, PartisanPort, H}, NodesAcc) ->
                    Node = list_to_atom(atom_to_list(?NODENAME)++"@"++H),
                    %% we could also do this by querying
                    %% the srv records of _data._tcp.vonnegut.default.svc.cluster.local
                    ClientPort = rpc:call(Node, vg_config, port, []),
                    ordsets:add_element({Node, H, PartisanPort, ClientPort}, NodesAcc)
                end, ordsets:new(), inet_res:lookup(DiscoveryDomain, in, srv)).

%%%-------------------------------------------------------------------
%% @doc Track the current state of the chain this node is a member of.
%%
%% @end
%%%-------------------------------------------------------------------
-module(vg_chain_state).

-compile([warn_missing_spec_all, warn_untyped_record]).

-behaviour(gen_statem).

-export([start_link/0,
         next/0,
         head/0]).

-export([init/1,
         active/3,
         inactive/3,
         callback_mode/0,
         terminate/3,
         code_change/4]).

-include_lib("kernel/include/inet.hrl").

-type chain_name() :: atom().
-type role() :: head | tail | middle | solo | undefined.
-type chain_node() :: {atom(), string(), inet:port_number(), inet:port_number()}.

-export_type([chain_name/0, role/0,
               chain_node/0]).

-record(data, {
          name         :: chain_name(),
          role         :: role(),
          head         :: node(),
          cluster_type :: vg_config:cluster_type(),
          members = [] :: ordsets:ordset(node()),
          all_nodes    :: [chain_node()] | undefined,
          next_node    :: atom() | tail,
          replicas     :: integer()
         }).

-define(SERVER, ?MODULE).
-define(NODENAME, vonnegut).

-spec start_link() -> gen_statem:start_ret().
start_link() ->
    gen_statem:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec next() -> node().
next() ->
    gen_statem:call(?SERVER, next_node).

-spec head() -> node().
head() ->
    gen_statem:call(?SERVER, head).

-spec init([]) -> {ok, inactive, #data{}, [gen_statem:action()]}.
init([]) ->
    ChainName = vg_config:chain_name(),
    ClusterType = vg_config:cluster_type(),
    Replicas = vg_config:replicas(),
    {ok, inactive, #data{name=ChainName,
                         replicas=Replicas,
                         cluster_type=ClusterType}, [{state_timeout, 0, connect}]}.

-spec inactive(gen_statem:event_type() | enter, term(), #data{}) -> gen_statem:event_handler_result(atom()).
inactive(enter, _, _Data) ->
    prometheus_boolean:set(is_active, false),
    keep_state_and_data;
inactive({call, From}, next_node, _Data) ->
    {keep_state_and_data, [{reply, From, undefined}]};
inactive({call, From}, head, _Data) ->
    {keep_state_and_data, [{reply, From, undefined}]};
inactive(state_timeout, connect, Data=#data{name=Name,
                                            replicas=Replicas,
                                            cluster_type=ClusterType}) ->
    {Members, AllNodes} = join(ClusterType),
    logger:info("cluster_type=~p members=~p all_nodes=~p", [ClusterType, Members, AllNodes]),
    case {whereis(vg_topics_sup), role(node(), Members, Replicas, ClusterType)} of
        {undefined, _} ->
            {keep_state, Data#data{members=Members,
                                   role=undefined}, [{state_timeout, 1000, connect}]};
        {_P, solo} when is_pid(_P) ->
            logger:info("at=chain_complete role=solo requested_size=1", []),
            logger:info("at=start_cluster_mgr role=solo"),
            vonnegut_sup:start_acceptor_pool(solo),
            ClientPort = vg_config:port(),
            PartisanPort = application:get_env(partisan, peer_port, 10200),
            [N, H] = string:split(atom_to_list(node()), "@"),
            vonnegut_sup:start_cluster_mgr(solo, [{list_to_atom(N), H, PartisanPort, ClientPort}]),
            {next_state, active, Data#data{members=Members,
                                           role=solo,
                                           head=node(),
                                           all_nodes=[],
                                           next_node=tail}};
        {_P, undefined} when is_pid(_P) ->
            {keep_state, Data#data{members=Members,
                                   role=undefined}, [{state_timeout, 1000, connect}]};
        {_P, Role} when is_pid(_P) ->
            logger:info("at=chain_join role=~s members=~p", [Role, Members]),
            case length(Members) of
                Size when Size >= Replicas ->
                    case Role of
                        head ->
                            logger:info("at=start_cluster_mgr role=head"),
                            %% if cluster mgr isn't running, start it
                            %% otherwise, add this chain to the cluster mgr
                            %% and all our topics
                            vonnegut_sup:start_cluster_mgr(Name, AllNodes);
                        _ ->
                            ok
                    end,

                    vonnegut_sup:start_acceptor_pool(Role),

                    %% monitor next link in the chain
                    NextNode = next_node(Role, node(), Members),
                    case string:split(atom_to_list(NextNode), "@") of
                        [N, H] ->
                            [Port] = [P || {N1, H1, _, P} <- AllNodes,
                                           N1 =:= list_to_atom(N),
                                           H1 =:= H],
                            vg_client_pool:start_pool(next_brick, #{ip => H,
                                                                    port => Port});
                        _ ->
                            ok
                    end,

                    Self = self(),
                    vg_peer_service:on_down(NextNode, fun() -> Self ! {next_node_down, NextNode} end),

                    logger:info("at=chain_complete requested_size=~p", [Replicas]),
                    {next_state, active, Data#data{members=Members,
                                                   head=hd(Members),
                                                   all_nodes=AllNodes,
                                                   role=Role,
                                                   next_node=NextNode}};
                Size ->
                    logger:info("at=chain_incomplete requested_size=~p current_size=~p", [Replicas, Size]),
                    {keep_state, Data#data{members=Members,
                                           role=Role}, [{state_timeout, 1000, connect}]}
            end
    end;
inactive(info, {next_node_down, NextNode}, _Data) ->
    logger:info("state=inactive next_node_down=~p", [NextNode]),
    {keep_state_and_data, [{state_timeout, 0, connect}]}.

-spec active(gen_statem:event_type() | enter, term(), #data{}) -> gen_statem:event_handler_result(atom()).
active(enter, _, #data{role=Role, replicas=Replicas}) ->
    set_metrics(Role, Replicas),
    keep_state_and_data;
active({call, From}, next_node, #data{next_node=NextNode}) ->
    {keep_state_and_data, [{reply, From, NextNode}]};
active({call, From}, head, #data{head=Head}) ->
    {keep_state_and_data, [{reply, From, Head}]};
active(info, {next_node_down, NextNode}, Data) ->
    logger:info("state=active next_node_down=~p", [NextNode]),
    {next_state, inactive, Data, 0}.

-spec callback_mode() -> [state_functions | state_enter].
callback_mode() ->
    [state_functions, state_enter].

-spec terminate(term(), atom(), #data{}) -> ok.
terminate(_Reason, _State, _Data) ->
    ok.

-spec code_change(term(), atom(), #data{}, term()) -> {ok, atom(), #data{}}.
code_change(_, State, Data, _) ->
    {ok, State, Data}.

%% Internal functions

%% assume we expect to find at least 1 node if using srv discovery
-spec role(node(), [node()], pos_integer(), vg_config:cluster_type()) -> role().
role(_Node, _, 1, _) ->
    solo;
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

-spec next_node(role(), node(), [node()]) -> node().
next_node(tail, _, _) ->
    tail;
next_node(head, _, [_, Next | _]) ->
    Next;
next_node(_, Node, []) ->
    Node;
next_node(_, _, [N]) ->
    N;
next_node(_, Node, Nodes) ->
    find_next(Node, Nodes).

-spec find_next(node(), ordsets:ordset(node())) -> node().
find_next(Node, Nodes) ->
    try
        %% set the accumulator when the node we are looking
        %% for the next of is found and throw to return
        %% the first element encountered after the acc is set
        lists:foldl(fun(N, none) when Node =:= N ->
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

-spec join(vg_config:cluster_type()) -> {[node()], [chain_node()]}.
join(ClusterType) ->
    AllNodes = lookup(ClusterType),
    ordsets:fold(fun({Name, Host, PartisanPort, _ClientPort}, _) ->
                     Node = list_to_atom(atom_to_list(Name)++"@"++Host),
                     IP = case inet:parse_address(Host) of
                              {error, einval} ->
                                  {ok, #hostent{h_addr_list=[IPAddress | _]}} = inet_res:getbyname(Host, a),
                                  IPAddress;
                              {ok, IPAddress} ->
                                  IPAddress
                          end,
                     N = #{name => Node,
                           listen_addrs => [#{ip => IP, port => PartisanPort}],
                           parallelism => 1},
                     vg_peer_service:join(N)
                 end, ok, AllNodes),
    {ok, Members} = vg_peer_service:members(),
    {lists:usort(Members), AllNodes}.

%% leave() ->
%%     vg_peer_service:leave([]).

%%

-spec lookup(vg_config:cluster_type()) -> [chain_node()].
lookup(local) ->
    ordsets:new();
lookup(none) ->
    ordsets:new();
lookup({direct, Nodes}) ->
    ordsets:from_list(Nodes);
lookup({srv, DiscoveryDomain}) ->
    lists:foldl(fun({_, _, PartisanPort, H}, NodesAcc) when is_integer(PartisanPort), is_list(H) ->
                    Node = list_to_atom(atom_to_list(?NODENAME)++"@"++H),
                    %% we could also do this by querying
                    %% the srv records of _data._tcp.vonnegut.default.svc.cluster.local
                    ClientPort = rpc:call(Node, vg_config, port, []),
                    true = is_integer(ClientPort),
                    ordsets:add_element({?NODENAME, H, PartisanPort, ClientPort}, NodesAcc)
                end, ordsets:new(), inet_res:lookup(DiscoveryDomain, in, srv)).

-spec set_metrics(role(), pos_integer()) -> ok.
set_metrics(Role, Replicas) ->
    prometheus_boolean:set(is_active, true),
    prometheus_gauge:set(replicas, Replicas),
    RoleMetric = role_metric(Role),
    [prometheus_boolean:set(B, false) || B <- [is_solo,
                                               is_head,
                                               is_tail,
                                               is_middle], B =/= RoleMetric],
    prometheus_boolean:set(RoleMetric, true).

-spec role_metric(role()) -> is_solo | is_head | is_tail | is_middle.
role_metric(solo) ->
    is_solo;
role_metric(head) ->
    is_head;
role_metric(tail) ->
    is_tail;
role_metric(middle) ->
    is_middle.

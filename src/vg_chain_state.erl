-module(vg_chain_state).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
          role      :: head | tail | middle | solo | undefined,
          members   :: ordsets:ordset(),
          next_node :: atom() | tail,
          replicas  :: integer(),
          active    :: boolean()
         }).

-define(SERVER, ?MODULE).
-define(NODENAME, "vonnegut").

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    Replicas = list_to_integer(application:get_env(vonnegut, replicas, "1")),
    {ok, #state{replicas=Replicas,
                active=false}, 0}.

handle_call(_, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State=#state{replicas=Replicas}) ->
    Members = join(),
    Role = role(node(), Members),
    NextNode = next_node(Role, node(), Members),
    Self = self(),
    vg_peer_service:on_down(NextNode, fun() -> Self ! {next_node_down, NextNode} end),
    lager:info("at=chain_join role=~s next_node=~p members=~p", [Role, NextNode, Members]),

    {ok, CurrentMembers} = vg_peer_service:members(),
    case length(CurrentMembers) of
        Replicas ->
            lager:info("at=chain_complete requested_size=~p", [Replicas]),
            {noreply, State#state{members=Members,
                                  role=Role,
                                  next_node=NextNode,
                                  active=true}};
        Size ->
            lager:info("at=chain_incomplete requested_size=~p current_size=~p", [Replicas, Size]),
            {noreply, State#state{members=Members,
                                  role=Role,
                                  next_node=NextNode,
                                  active=false}, 100}
    end;
handle_info({next_node_down, NextNode}, State) ->
    lager:info("next_node_down=~p", [NextNode]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

%% Internal functions

role(_Node, []) ->
    solo;
role(Node, [Node]) ->
    solo;
role(Node, [Node | _]) ->
    head;
role(Node, Nodes) ->
    case lists:reverse(Nodes) of
        [Node | _] ->
            tail;
        _ ->
            middle
    end.

next_node(solo, _, _) ->
    tail;
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


join() ->
    ClusterType = case application:get_env(vonnegut, cluster, local) of
                      "local" ->
                          local;
                      local ->
                          local;
                      {direct, Nodes} ->
                          {direct, Nodes};
                      {srv, Domain} ->
                          {srv, Domain};
                      Other ->
                          lager:error("Unknown clustering option: ~p", [Other]),
                          none
                  end,

    NewNodes = lookup(ClusterType),
    ordsets:fold(fun(Node, _) -> vg_peer_service:join(Node) end, ok, NewNodes),
    NewNodes.

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
    lists:foldl(fun({_, _, _, H}, NodesAcc) ->
                    ordsets:add_element(list_to_atom(string:join([?NODENAME, H], "@")), NodesAcc)
                end, ordsets:new(), inet_res:lookup(DiscoveryDomain, in, srv)).

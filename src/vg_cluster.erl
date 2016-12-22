-module(vg_cluster).

-export([join/0,
         leave/0]).

-define(NODENAME, "vonnegut").

join() ->
    DiscoveryDomain = case application:get_env(vonnegut, discovery_domain, local) of
                          "local" ->
                              local;
                          Other ->
                              Other
                      end,

    %% ip or fqdn
    NodeNameType = application:get_env(vonnegut, node_name_type, none),

    NewNodes = lookup(NodeNameType, DiscoveryDomain),
    sets:fold(fun(Node, _) -> net_kernel:start([Node]) end, ok, NewNodes).

leave() ->
    lasp_peer_service:leave([]).

%%

lookup(_, local) ->
    sets:new();
lookup(none, _) ->
    sets:new();
lookup(direct, Domain) ->
    sets:add_element(list_to_atom(string:join([?NODENAME, Domain], "@")), sets:new());
lookup(literal, Domain) ->
    sets:add_element(list_to_atom(Domain), sets:new());
lookup(ip, DiscoveryDomain) ->
    lists:foldl(fun(Record, NodesAcc) ->
                    H = inet_parse:ntoa(Record),
                    sets:add_element(list_to_atom(string:join([?NODENAME, H], "@")), NodesAcc)
                end, sets:new(), inet_res:lookup(DiscoveryDomain, in, a));
lookup(fqdns, DiscoveryDomain) ->
    lists:foldl(fun(Record, NodesAcc) ->
                    H = extract_host(inet_res:gethostbyaddr(Record)),
                    sets:add_element(list_to_atom(string:join([?NODENAME, H], "@")), NodesAcc)
                end, sets:new(), inet_res:lookup(DiscoveryDomain, in, a)).

extract_host({ok, {hostent, FQDN, _, _, _, _}}) ->
    FQDN;
extract_host(_) ->
    error.

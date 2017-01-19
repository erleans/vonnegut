-module(vg_peer_service).

-export([join/1,
         leave/0,
         on_down/2,
         members/0,
         manager/0,
         stop/0,
         stop/1]).

join(Node) ->
    {Name, Host, Port} = Node,
    %% also need to handle fqdn here, but I think it's best fixed in partisan
    Host1 = string:tokens(Host, "."),
    HostIP0 = [list_to_integer(Octet) || Octet <- Host1],
    HostIP = list_to_tuple(HostIP0),
    %% this assumption of correlation is bad and we should fix; need a
    %% better to do sys.config variations...
    application:set_env(partisan, peer_port, Port + 10000),
    partisan_peer_service:join({Name, HostIP, Port + 10000}, true).

leave() ->
    partisan_peer_service:leave([]).

on_down(Name, Fun) ->
    partisan_default_peer_service_manager:on_down(Name, Fun).

members() ->
    partisan_peer_service:members().

manager() ->
    partisan_peer_service:manager().

stop() ->
    partisan_peer_service:stop("received stop request").

stop(Reason) ->
    partisan_peer_service:stop(Reason).

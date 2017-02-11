-module(vg_peer_service).

-export([join/1,
         leave/0,
         on_down/2,
         members/0,
         manager/0,
         stop/0,
         stop/1]).

join(Node) ->
    partisan_peer_service:join(Node, true).

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

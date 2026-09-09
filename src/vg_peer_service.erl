-module(vg_peer_service).

-compile([warn_missing_spec_all, warn_untyped_record]).

-export([join/1,
         leave/0,
         on_down/2,
         members/0,
         manager/0,
         stop/0,
         stop/1]).

-spec join(map() | node()) -> ok | {error, term()}.
join(Node) ->
    partisan_peer_service:join(Node).

-spec leave() -> ok | {error, term()}.
leave() ->
    partisan_peer_service:leave([]).

-spec on_down(node(), fun(() -> term())) -> ok | {error, term()}.
on_down(Name, Fun) ->
    partisan_default_peer_service_manager:on_down(Name, Fun).

-spec members() -> {ok, [node()]}.
members() ->
    partisan_peer_service:members().

-spec manager() -> module().
manager() ->
    partisan_peer_service:manager().

-spec stop() -> ok.
stop() ->
    partisan_peer_service:stop("received stop request").

-spec stop(string()) -> ok.
stop(Reason) ->
    partisan_peer_service:stop(Reason).

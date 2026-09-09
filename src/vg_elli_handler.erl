-module(vg_elli_handler).

-compile([warn_missing_spec_all, warn_untyped_record]).

-export([handle/2,
         handle_event/3]).

-include_lib("elli/include/elli.hrl").
-behaviour(elli_handler).

-spec handle(elli:req(), term()) -> {ok | 404, [], binary()}.
handle(Req, _Args) ->
    %% Delegate to our handler function
    handle(Req#req.method, elli_request:path(Req), Req).

-spec handle(elli:http_method(), [binary()], elli:req()) -> {ok | 404, [], binary()}.
handle('GET', [<<"_health">>], _Req) ->
    {ok, [], <<"It's all good.">>};

handle(_, _, _Req) ->
    {404, [], <<"Not Found">>}.

-spec handle_event(atom(), term(), term()) -> ok.
handle_event(_Event, _Data, _Args) ->
    ok.

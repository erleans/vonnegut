%%%-------------------------------------------------------------------
%% @doc vonnegut public API
%% @end
%%%-------------------------------------------------------------------

-module(vonnegut_app).

-compile([warn_missing_spec_all, warn_untyped_record]).

-behaviour(application).

%% Application callbacks
-export([start/2,
         stop/1]).

%%====================================================================
%% API
%%====================================================================

-spec start(application:start_type(), term()) -> {ok, pid()} | {error, term()}.
start(_StartType, _StartArgs) ->
    init_tables(),
    case vonnegut_sup:start_link() of
        {ok, Pid} -> {ok, Pid};
        {error, Reason} -> {error, Reason}
    end.


%%--------------------------------------------------------------------
-spec stop(term()) -> ok.
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

-spec init_tables() -> ets:table().
init_tables() ->
    vg_log_segments:init_table(),
    vg_topics:init_table().


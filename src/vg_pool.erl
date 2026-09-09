-module(vg_pool).

-compile([warn_missing_spec_all, warn_untyped_record]).

-behaviour(acceptor_pool).

-export([start_link/1,
         accept_socket/2]).

-export([init/1]).

%% public api

-spec start_link(vg_chain_state:role()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Role) ->
    acceptor_pool:start_link({local, ?MODULE}, ?MODULE, [Role]).

-spec accept_socket(inet:socket(), pos_integer()) -> {ok, reference()} | {error, term()}.
accept_socket(Socket, Acceptors) ->
    acceptor_pool:accept_socket(?MODULE, Socket, Acceptors).

%% acceptor_pool api

-spec init([vg_chain_state:role()]) -> {ok, {map(), [map()]}}.
init([Role]) ->
    Conn = #{id => vg_conn,
             start => {vg_conn, [Role], []},
             grace => 5000}, % Give connections 5000ms to close before shutdown
    {ok, {#{}, [Conn]}}.

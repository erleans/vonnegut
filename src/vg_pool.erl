-module(vg_pool).

-behaviour(acceptor_pool).

-export([start_link/1,
         accept_socket/2]).

-export([init/1]).

%% public api

start_link(Role) ->
    acceptor_pool:start_link({local, ?MODULE}, ?MODULE, [Role]).

accept_socket(Socket, Acceptors) ->
    acceptor_pool:accept_socket(?MODULE, Socket, Acceptors).

%% acceptor_pool api

init([Role]) ->
    Conn = #{id => vg_conn,
             start => {vg_conn, [Role], []},
             grace => 5000}, % Give connections 5000ms to close before shutdown
    {ok, {#{}, [Conn]}}.

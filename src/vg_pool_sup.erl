-module(vg_pool_sup).

-behaviour(supervisor).

%% public api

-export([start_link/0]).

%% supervisor api

-export([init/1]).

%% public api

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% supervisor api

init([]) ->
    Flags = #{strategy => rest_for_one},
    Pool = #{id => vg_pool,
             start => {vg_pool, start_link, []}},
    Socket = #{id => vg_socket,
               start => {vg_socket, start_link, []}},
    {ok, {Flags, [Pool, Socket]}}.

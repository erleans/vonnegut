-module(vg_pool_sup).

-compile([warn_missing_spec_all, warn_untyped_record]).

-behaviour(supervisor).

%% public api

-export([start_link/1]).

%% supervisor api

-export([init/1]).

%% public api

-spec start_link(vg_chain_state:role()) -> supervisor:startlink_ret().
start_link(Role) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Role]).

%% supervisor api

-spec init([vg_chain_state:role()]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([Role]) ->
    Flags = #{strategy => rest_for_one},
    Pool = #{id => vg_pool,
             start => {vg_pool, start_link, [Role]}},
    Socket = #{id => vg_socket,
               start => {vg_socket, start_link, []}},
    {ok, {Flags, [Pool, Socket]}}.

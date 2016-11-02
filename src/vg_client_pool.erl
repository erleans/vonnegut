-module(vg_client_pool).

-export([start/0]).

start() ->
    SocketOpts = [binary,
                  {buffer, 65535},
                  {nodelay, true},
                  {packet, raw},
                  {send_timeout, 5000},
                  {send_timeout_close, true}],
    shackle_pool:start(vg_client_pool, vg_client, [{ip, "127.0.0.1"},
                                                   {port, 5555},
                                                   {reconnect, true},
                                                   {reconnect_time_max, 120000},
                                                   {reconnect_time_min, none},
                                                   {socket_options, SocketOpts}],
                       [{backlog_size, 1024},
                        {pool_size, 2},
                        {pool_strategy, random}]).

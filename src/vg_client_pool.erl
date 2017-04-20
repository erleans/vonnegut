-module(vg_client_pool).

-export([start/0,
         stop/0,
         get_pool/0,
         get_pool/2,
         refresh_topic_map/0]).

-ifdef(TEST).
-export([start_pool/2]).
-endif.

-include("vg.hrl").

-define(OPTIONS, [set, public, named_table, {read_concurrency, true}]).

start() ->
    %% maybe start this if it hasn't been
    application:ensure_all_started(shackle),
    case application:get_env(vonnegut, client) of
        {ok, ClientConfig} ->
            case proplists:get_value(endpoints, ClientConfig) of
                undefined ->
                    lager:error("No endpoints configured for client");
                [{Host, Port} | _] ->
                    start_pool(metadata, #{ip => Host,
                                           port => Port}),
                    {ok, {Chains, Topics}} = vg_client:metadata(),
                    maybe_init_ets(),
                    _ = start_pools(Chains),
                    application:set_env(vonnegut, chains, Chains),
                    refresh_topic_map(Topics, Chains),
                    ok
            end;
        _ ->
            lager:info("No client configuration")
    end.

start_pools(Chains) ->
    [begin
         lager:debug("starting chain: ~p", [Chain]),
         HeadName = make_pool_name(Name, write),
         {HeadHost, HeadPort} = Chain#chain.head,
         start_pool(HeadName, #{ip => HeadHost,
                                port => HeadPort}),
         TailName = make_pool_name(Name, read),
         {TailHost, TailPort} = Chain#chain.tail,
         start_pool(TailName, #{ip => TailHost,
                                port => TailPort})
     end
     || {Name, Chain} <- maps:to_list(Chains)].

refresh_topic_map() ->
    {ok, {Chains, Topics}} = vg_client:metadata(),
    _ = start_pools(Chains),
    refresh_topic_map(Topics, Chains).

refresh_topic_map(Topics, Chains) ->
    lager:debug("refreshing client topic map", []),
    maybe_init_ets(clean),
    [make_pool_name(Chain, read) || Chain <- maps:keys(Chains)],
    _ = [ets:insert(?topic_map, {Topic, Chain}) || {Topic, Chain} <- maps:to_list(Topics)].

get_pool() ->
    Key = ets:first(?topic_map),
    get_pool(Key, read).

get_pool(Topic, RW) ->
    get_pool(Topic, RW, first).

get_pool(Topic, RW, Try) ->
    %% at some point we should handle retries here for when the topic
    %% list is being refreshed.
    case ets:lookup(?topic_map, Topic) of
        [] ->
            case {RW, Try} of
                {write, first} ->
                    vg_client:ensure_topic(Topic),
                    refresh_topic_map(),
                    get_pool(Topic, RW, second);
                {read, first} ->
                    refresh_topic_map(),
                    get_pool(Topic, RW, second);
                {_, second} ->
                    lager:error("failed finding existing chain and creating new topic=~p", [Topic]),
                    {error, not_found}
            end;
        [{_, Chain}] ->
            Pool = make_pool_name(Chain, RW),
            lager:debug("found chain for topic=~p on pool=~p", [Topic, Pool]),
            {ok, Pool}
    end.

make_pool_name(Chain, read) ->
    binary_to_atom(<<Chain/binary, "_tail">>, utf8);
make_pool_name(Chain, write) ->
    binary_to_atom(<<Chain/binary, "_head">>, utf8).

maybe_init_ets() ->
    maybe_init_ets(foo).

%% eventually handle the clean argument
maybe_init_ets(_) ->
    case ets:info(?topic_map, name) of
        undefined ->
            ets:new(?topic_map, ?OPTIONS);
        _ ->
            ok
    end.

start_pool(Name, Opts) ->
    ClientPoolSize = application:get_env(vonnegut, client_pool_size, 10),
    SocketOpts = [binary,
                  {buffer, 65535},
                  {nodelay, true},
                  {packet, raw},
                  {send_timeout, 5000},
                  {send_timeout_close, true}],
    shackle_pool:start(Name, vg_client,
                       [{ip, maps:get(ip, Opts, "127.0.0.1")},
                        {port, maps:get(port, Opts, 5555)},
                        {reconnect, true},
                        {reconnect_time_max, 120000},
                        {reconnect_time_min, none},
                        {socket_options, SocketOpts}],
                       [{backlog_size, 1024},
                        {pool_size, ClientPoolSize},
                        {pool_strategy, round_robin}]).

stop() ->
    [shackle_pool:stop(Pool)
     || Pool <- application:get_env(vonnegut, client_pools, [])].

%%%%%%%%%%%%%%%%%%

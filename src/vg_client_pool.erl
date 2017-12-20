-module(vg_client_pool).

-export([start/0, start/1,
         stop/0,
         restart/0,
         get_pool/2,
         start_pool/2,
         make_pool_name/2,
         refresh_topic_map/0]).

-include("vg.hrl").

-define(OPTIONS, [set, public, named_table, {read_concurrency, true}]).

start() ->
    start(#{}).

start(Opts) ->
    %% so restarts won't lose settings
    application:set_env(vonnegut, global_pool_opts, Opts),
    start(Opts, 0).

start(_Opts, 10) ->
    {error, could_not_start_pools};
start(Opts, N) ->
    %% maybe start this if it hasn't been
    application:ensure_all_started(shackle),
    case application:get_env(vonnegut, client) of
        {ok, ClientConfig} ->
            case proplists:get_value(endpoints, ClientConfig) of
                undefined ->
                    lager:error("No endpoints configured for client");
                [{Host, Port} | _] ->
                    start_pool(metadata, Opts#{ip => Host,
                                               port => Port}),
                    try
                        case vg_client:topics() of
                            {ok, {_, Chains}} ->
                                maybe_init_ets(),
                                _ = start_pools(Chains),
                                application:set_env(vonnegut, chains, Chains),
                                refresh_topic_map(),
                                ok
                        end
                    catch _:_Reason ->
                            lager:warning("at=start_pools error=~p", [_Reason]),
                            timer:sleep(500),
                            start(Opts, N + 1)
                    end
            end;
        _ ->
            lager:info("No client configuration")
    end.

start_pools(Chains) ->
    [begin
         Name = <<HeadHost/binary, "-", (integer_to_binary(HeadPort))/binary>>,
         lager:info("starting chain: ~p ~p", [Name, C]),
         HeadName = make_pool_name(Name, write),
         start_pool(HeadName, #{ip => binary_to_list(HeadHost),
                                port => HeadPort}),
         TailHost =
             case TailHost0 of
                 <<"solo">> -> HeadHost;
                 _ -> TailHost0
             end,
         %% the name of the pool can be misleading as to what host and
         %% port it's on.  Do we need to fix this?
         TailName = make_pool_name(Name, read),
         start_pool(TailName, #{ip => binary_to_list(TailHost),
                                port => TailPort})
     end
     || #{name := _Name,
          head := {HeadHost, HeadPort},
          tail := {TailHost0, TailPort}} = C <- Chains].

refresh_topic_map() ->
    %% TODO live migrate pools when the chain list changes?
    %% or just restart the whole mess?
    {ok, {_, Chains}} = vg_client:topics(),
    maybe_init_ets(clean),
    ets:insert(?topic_map, {chains, Chains}),
    ets:insert(?topic_map, {lookup, lookup_list(Chains)}).

lookup_list(Chains) ->
    [begin
         Name = <<HeadHost/binary, "-", (integer_to_binary(HeadPort))/binary>>,
         HeadName = make_pool_name(Name, write),
         TailName = make_pool_name(Name, read),
         {Start, End, HeadName, TailName}
     end
     || #{topics_start := Start,
          topics_end := End,
          head := {HeadHost, HeadPort}} <- Chains].

get_pool(Topic, RW) ->
    %% at some point we should handle retries here for when the topic
    %% list is being refreshed.
    case ets:lookup(?topic_map, lookup) of
        [] ->
            refresh_topic_map(),
            get_pool(Topic, RW);
        [{_, Chains}] ->
            case find_chain(Chains, Topic, RW) of
                {ok, Pool} ->
                    lager:debug("found chain for topic=~p on pool=~p", [Topic, Pool]),
                    {ok, Pool};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% TODO: work out how to replace this with a select, maybe
find_chain([], _Topic, _RW) ->
    {error, malformed_chain};
find_chain([{start_space, end_space, HeadName, TailName} | _Tail], _Topic, RW)  ->
    {ok, pick_pool(HeadName, TailName, RW)};
find_chain([{start_space, E, HeadName, TailName} | _Tail], Topic, RW) when Topic =< E ->
    {ok, pick_pool(HeadName, TailName, RW)};
find_chain([{S, end_space, HeadName, TailName} | _Tail], Topic, RW) when Topic >= S ->
    {ok, pick_pool(HeadName, TailName, RW)};
find_chain([{S, E, HeadName, TailName} | _Tail], Topic, RW) when Topic >= S andalso Topic =< E ->
    {ok, pick_pool(HeadName, TailName, RW)};
find_chain([_|Tail], Topic, RW) ->
    find_chain(Tail, Topic, RW).

pick_pool(Head, _Tail, write) ->
    Head;
pick_pool(_Head, Tail, read) ->
    Tail.

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
                        {reconnect, maps:get(reconnect, Opts, true)},
                        {reconnect_time_max, 120000},
                        {reconnect_time_min, 250},
                        {socket_options, SocketOpts}],
                       [{backlog_size, 1024},
                        {pool_size, ClientPoolSize},
                        {pool_strategy, round_robin}]).

stop() ->
    [shackle_pool:stop(Pool)
     || Pool <- application:get_env(vonnegut, client_pools, [])],
    application:stop(shackle).

restart() ->
    stop(),
    Opts = application:get_env(vonnegut, global_pool_opts, #{}),
    start(Opts).
%%%%%%%%%%%%%%%%%%

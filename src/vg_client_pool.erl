-module(vg_client_pool).

-export([start/0,
         stop/0,
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
                    {ok, {_, Chains}} = vg_client:topics(),
                    maybe_init_ets(),
                    _ = start_pools(Chains),
                    application:set_env(vonnegut, chains, Chains),
                    refresh_topic_map(),
                    ok
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
          tail := {TailHost0, TailPort}} = C  <- Chains].

refresh_topic_map() ->
    %% get a list of chains
    {ok, {_, Chains}} = vg_client:topics(),
    maybe_init_ets(clean),
    ets:insert(?topic_map, {chains, Chains}).

get_pool(Topic, RW) ->
    %% at some point we should handle retries here for when the topic
    %% list is being refreshed.
    case ets:lookup(?topic_map, Topic) of
        [] ->
            %% TODO: this creates a topic on a read, which isn't great
            case vg_client:ensure_topic(Topic) of
                {ok, {_Chains, #{Topic := Chain}}} ->
                {ok, R} = vg_client:topics(),
                    [{chains, _Chains2}] =  ets:lookup(?topic_map, chains),
                    lager:info("chains ~p", [R]),
                    ets:insert(?topic_map, {Topic, Chain}),
                    Pool = make_pool_name(Chain, RW),
                    lager:info("pool ~p chain ~p", [Pool, Chain]),
                    {ok, Pool};
                {error, Reason} ->
                    {error, Reason}
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

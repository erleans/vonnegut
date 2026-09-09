-module(vg_client_pool).

-compile([warn_missing_spec_all, warn_untyped_record]).

-export([start/0, start/1,
         stop/0,
         restart/0,
         get_pool/2,
         start_pool/2,
         make_pool_name/2,
         refresh_topic_map/0]).

-include("vg.hrl").

-type pool_options() :: #{ip => string() | inet:ip_address(),
                          port => inet:port_number(), reconnect => boolean()}.
-type access_mode() :: read | write.
-type lookup_entry() :: {vg_protocol:chain_boundary(), vg_protocol:chain_boundary(), atom(), atom()}.
-export_type([pool_options/0, access_mode/0]).

-define(OPTIONS, [set, public, named_table, {read_concurrency, true}]).

-spec start() -> ok | {error, term()}.
start() ->
    start(#{}).

-spec start(pool_options()) -> ok | {error, term()}.
start(Opts) ->
    %% so restarts won't lose settings
    application:set_env(vonnegut, global_pool_opts, Opts),
    start(Opts, 0).

-spec start(pool_options(), non_neg_integer()) -> ok | {error, term()}.
start(_Opts, 10) ->
    {error, could_not_start_pools};
start(Opts, N) ->
    %% maybe start this if it hasn't been
    application:ensure_all_started(shackle),
    case application:get_env(vonnegut, client) of
        {ok, ClientConfig} ->
            case proplists:get_value(endpoints, ClientConfig) of
                undefined ->
                    logger:error("No endpoints configured for client");
                [{Host, Port} | _] when is_integer(Port) ->
                    start_(Opts, N, Host, Port);
                [HostPort | _] when is_list(HostPort) ->
                    case parse_host_port(HostPort) of
                        {ok, Host, Port} -> start_(Opts, N, Host, Port);
                        {error, _} -> logger:error("Invalid endpoint ~p", HostPort)
                    end
            end;
        _ ->
            logger:info("No client configuration")
    end.

-spec start_(pool_options(), non_neg_integer(), string(), inet:port_number()) -> ok | {error, term()}.
start_(Opts, N, Host, Port) ->
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
  catch
      ?WITH_STACKTRACE(_, R, S)
          logger:warning("at=start_pools error=~p stacktrace=~p", [R, S]),
          timer:sleep(500),
          start(Opts, N + 1)
  end.

-spec parse_host_port(string()) -> {ok, string(), integer()} | {error, invalid_host_port_string}.
parse_host_port(HostPortString) ->
    case string:split(HostPortString, ":", all) of
        [Host] ->
            {ok, Host, ?DEFAULT_PORT};
        [Host, Port] ->
            case string:to_integer(Port) of
                {IntegerPort, ""} -> {ok, Host, IntegerPort};
                {_, _}            -> {error, invalid_host_port_string}
            end;
        [_|_] ->
            {error, invalid_host_port_string}
    end.
  
-spec start_pools([vg_protocol:wire_chain()]) -> [ok | {error, term()}].
start_pools(Chains) ->
    [begin
         Name = <<HeadHost/binary, "-", (integer_to_binary(HeadPort))/binary>>,
         logger:info("starting chain: ~p ~p", [Name, C]),
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

-spec refresh_topic_map() -> true.
refresh_topic_map() ->
    %% TODO live migrate pools when the chain list changes?
    %% or just restart the whole mess?
    {ok, {_, Chains}} = vg_client:topics(),
    maybe_init_ets(clean),
    ets:insert(?topic_map, {chains, Chains}),
    ets:insert(?topic_map, {lookup, lookup_list(Chains)}).

-spec lookup_list([vg_protocol:wire_chain()]) -> [lookup_entry()].
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

-spec get_pool(vg:topic(), access_mode()) -> {ok, atom()} | {error, malformed_chain}.
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
                    logger:debug("found chain for topic=~p on pool=~p", [Topic, Pool]),
                    {ok, Pool};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% TODO: work out how to replace this with a select, maybe
-spec find_chain([lookup_entry()], vg:topic(), access_mode()) -> {ok, atom()} | {error, malformed_chain}.
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

-spec pick_pool(atom(), atom(), access_mode()) -> atom().
pick_pool(Head, _Tail, write) ->
    Head;
pick_pool(_Head, Tail, read) ->
    Tail.

-spec make_pool_name(binary(), access_mode()) -> atom().
make_pool_name(Chain, read) ->
    binary_to_atom(<<Chain/binary, "_tail">>, utf8);
make_pool_name(Chain, write) ->
    binary_to_atom(<<Chain/binary, "_head">>, utf8).

-spec maybe_init_ets() -> ets:table() | ok.
maybe_init_ets() ->
    maybe_init_ets(foo).

%% eventually handle the clean argument
-spec maybe_init_ets(term()) -> ets:table() | ok.
maybe_init_ets(_) ->
    case ets:info(?topic_map, name) of
        undefined ->
            ets:new(?topic_map, ?OPTIONS);
        _ ->
            ok
    end.

-spec start_pool(atom(), pool_options()) -> ok | {error, term()}.
start_pool(Name, Opts) ->
    ClientPoolSize = application:get_env(vonnegut, client_pool_size, 10),
    SocketOpts = [binary,
                  {buffer, 65535},
                  {nodelay, true},
                  {packet, raw},
                  {send_timeout, 5000},
                  {send_timeout_close, true}],
    Pools = application:get_env(vonnegut, client_pools, []),
    application:set_env(vonnegut, client_pools, [Name | Pools]),
    shackle_pool:start(Name, vg_client,
                       [{ip, maps:get(ip, Opts, "127.0.0.1")},
                        {port, maps:get(port, Opts, 5588)},
                        {reconnect, maps:get(reconnect, Opts, true)},
                        {reconnect_time_max, 120000},
                        {reconnect_time_min, 250},
                        {socket_options, SocketOpts}],
                       [{backlog_size, 1024},
                        {pool_size, ClientPoolSize},
                        {pool_strategy, round_robin}]).

-spec stop() -> ok | {error, term()}.
stop() ->
    [shackle_pool:stop(Pool)
     || Pool <- application:get_env(vonnegut, client_pools, [])],
    application:stop(shackle).

-spec restart() -> ok | {error, term()}.
restart() ->
    stop(),
    Opts = application:get_env(vonnegut, global_pool_opts, #{}),
    start(Opts).
%%%%%%%%%%%%%%%%%%

-module(vg_client_pool).

-export([start/0,
         stop/0,
         get_pool/2,
         refresh_topic_map/0]).

-define(topic_map, topic_map).

-define(OPTIONS, [set, public, named_table, {read_concurrency, true}]).

-record(chain, {
          name :: binary(),
          head_host :: inet:ip_address() | inet:hostname(),
          head_port :: inet:port_number(),
          tail_host :: inet:ip_address() | inet:hostname(),
          tail_port :: inet:port_number()
         }).

start() ->
    Cluster = get_cluster(),
    lager:info("cluster: ~p", [Cluster]),
    maybe_init_ets(),
    Chains = start_pools(Cluster),
    lager:info("chains: ~p", [Chains]),
    application:set_env(vonnegut, chains, Chains),
    refresh_topic_map(),
    lager:info("OK"),
    ok.

start_pools(Chains) ->
    [begin
         lager:info("starting chain: ~p", [Chain#chain.name]),
         HeadName = make_pool_name(Chain#chain.name, write),
         start_pool(HeadName, #{ip => Chain#chain.head_host,
                            port => Chain#chain.head_port}),
         TailName = make_pool_name(Chain#chain.name, read),
         start_pool(TailName, #{ip => Chain#chain.tail_host,
                            port => Chain#chain.tail_port}),
         Chain#chain.name
     end
     || Chain <- Chains].

refresh_topic_map() ->
    lager:info("refreshing client topic map", []),
    maybe_init_ets(clean),
    TopicList = [begin
                     Pool = make_pool_name(Chain, read),
                     Topics = shackle:call(Pool, topics),
                     {Chain, Topics}
                 end
                 || Chain <- application:get_env(vonnegut, chains, [])],
    _ = [[ets:insert(?topic_map, {Topic, Chain}) || Topic <- Topics]
         || {Chain, Topics} <- TopicList],
    %% hacky default,
    [{Chain1, _} | _] = TopicList,
    ets:insert(?topic_map, {<<>>, Chain1}).

get_pool(Topic, RW) ->
    %% at some point we should handle retries here for when the topic
    %% list is being refreshed.
    case ets:lookup(?topic_map, Topic) of
        [] ->
            case RW of
                write ->
                    %% just make it up for now  TODO: make this a settable
                    %% behavior serverside
                    [{_, Default}] = ets:lookup(?topic_map, <<>>),
                    {ok, make_pool_name(Default, RW)};
                read ->
                    {error, not_found}
            end;
        %%{error, not_found};
        [{_, Chain}] ->
            Pool = make_pool_name(Chain, RW),
            {ok, Pool}
    end.

make_pool_name(Chain, read) ->
    %% lager:info("making pool name ~p", [Chain]),
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
    ClientPoolSize = application:get_env(vonnegut, client_pool_size, 2),
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
                        {pool_strategy, random}]).

stop() ->
    [shackle_pool:stop(Pool)
     || Pool <- application:get_env(vonnegut, client_pools, [])].

%%%%%%%%%%%%%%%%%%

get_cluster() ->
    %% this works for now, but I guess we'll eventually need to move
    %% this client out into being something that someone else can use,
    %% when we'll need to change how the state gets in.
    Cluster =
        case application:get_env(vonnegut, cluster, undefined) of
            undefined ->
                %% we need a name here, what makes sense for single
                %% node testing?
                [#chain{head_host = "127.0.0.1",
                        head_port = 5555,
                        tail_host = "127.0.0.1",
                        tail_port = 5555}]; % assume local run if unset
            RawCluster ->
                raw_to_chains(RawCluster)
        end,
    Cluster.

raw_to_chains({direct, Nodes}) ->
    ParsedNodes = [parse_name(Node) || Node <- Nodes],
    Chains = ordsets:from_list([Chain || {Chain, _, _, _} <- ParsedNodes]),
    [make_chain(Chain, ParsedNodes)
     || Chain <- Chains].

parse_name({Name0, Host, Port}) ->
    %% this especially is likely to cause problems if we allow
    %% arbitrary chain naming
    Name = atom_to_list(Name0),
    [Chain, Pos] = string:tokens(Name, "-"),
    {Chain, list_to_integer(Pos), Host, Port}.

make_chain(Chain, Nodes0) ->
    Nodes = [Node || Node = {C, _, _, _} <- Nodes0, C =:= Chain],
    Len = length(Nodes),
    {value, {_, _, Head, HeadPort}} = lists:keysearch(0, 2, Nodes),
    {value, {_, _, Tail, TailPort}} = lists:keysearch(Len - 1, 2, Nodes),
    #chain{name = list_to_binary(Chain),
           head_host = Head,
           head_port = HeadPort,
           tail_host = Tail,
           tail_port = TailPort}.

-module(vg_conn).

-behaviour(acceptor).
-behaviour(gen_server).

%% acceptor api

-export([acceptor_init/3,
         acceptor_continue/3,
         acceptor_terminate/2]).

%% gen_server api

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

-define(FETCH_REQUEST, 1).

-type topic_partition() :: {binary(), [partition()]}.
-type partition() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}.

-record(state, {socket :: inets:socket(),
                ref    :: reference(),
                buffer :: binary()}).

acceptor_init(_SockName, LSocket, []) ->
    % monitor listen socket to gracefully close when it closes
    MRef = monitor(port, LSocket),
    {ok, MRef}.

acceptor_continue(_PeerName, Socket, MRef) ->
    gen_server:enter_loop(?MODULE, [], #state{socket=Socket, ref=MRef, buffer = <<>>}).

acceptor_terminate(Reason, _) ->
    % Something went wrong. Either the acceptor_pool is terminating or the
    % accept failed.
    exit(Reason).

%% gen_server api

init(_) ->
    {stop, acceptor}.

handle_call(Req, _, State) ->
    {stop, {bad_call, Req}, State}.

handle_cast(Req, State) ->
    {stop, {bad_cast, Req}, State}.

handle_info({tcp, Socket, Data}, State=#state{socket=Socket,
                                              buffer=Buffer}) ->
    NewBuffer = handle_data(<<Buffer/binary, Data/binary>>, Socket),
    ok = inet:setopts(Socket, [{active, once}]),
    {noreply, State#state{buffer=NewBuffer}};
handle_info({tcp_error, Socket, Reason}, State=#state{socket=Socket}) ->
    {stop, Reason, State};
handle_info({tcp_closed, Socket}, State=#state{socket=Socket}) ->
    {stop, normal, State};
handle_info({'DOWN', MRef, port, _, _}, State=#state{socket=Socket,
                                                     ref=MRef}) ->
    %% Listen socket closed, receive all pending data then stop. In more
    %% advanced protocols will likely be able to do better.
    error_logger:info_msg("Gracefully closing ~p~n", [Socket]),
    {stop, flush_socket(Socket), State};
handle_info(_, State) ->
    {noreply, State}.

code_change(_, State, _) ->
    {ok, State}.

terminate(_, _) ->
    ok.

%% internal

%% If a message can be fully read from the data then handle the request
%% else return the data to be kept in the buffer
-spec handle_data(binary(), inets:socket()) -> binary().
handle_data(<<Size:32/signed, Message:Size/binary, Rest/binary>>, Socket) ->
    handle_request(Message, Socket),
    Rest;
handle_data(Data, _Socket) ->
    Data.

%% Parse out the type of request (apikey) and the request data
handle_request(<<ApiKey:16/signed, _ApiVersion:16/signed, _CorrelationId:32/signed,
                 ClientIdSize:32/signed, _ClientId:ClientIdSize/binary, Request/binary>>, Socket) ->
    More = handle_request(ApiKey, Request, Socket),
    ok = inet:setopts(Socket, [{active, once}]),
    More.

handle_request(?FETCH_REQUEST, <<_ReplicaId:32/signed, _MaxWaitTime:32/signed,
                                 _MinBytes:32/signed, NumTopics:32/signed, TopicsRaw/binary>>, Socket) ->
    {[{Topic, [{Partition, Offset, _MaxBytes} | _]} | _], Rest} = parse_topics(NumTopics, TopicsRaw),

    {SegmentId, Position} = vg_utils:find_segment_offset(Topic, Partition, Offset),
    {ok, [LogDir]} = application:get_env(vonnegut, log_dirs),
    LogDir1 =  LogDir ++ atom_to_list(node()),
    TopicDir = filename:join(LogDir1, [binary_to_list(Topic), "-", integer_to_list(Partition)]),
    File = vg_utils:log_file(TopicDir, SegmentId),
    {ok, Fd} = file:open(File, [read, binary, raw]),
    {ok, _} = file:sendfile(Fd, Socket, Position, 0, []),
    file:close(Fd),

    Rest.


-spec parse_topics(non_neg_integer(), binary()) -> {[topic_partition()], binary()}.
parse_topics(Num, Raw) ->
    parse_topics(Num, Raw, []).

parse_topics(0, Rest, Topics) ->
    {Topics, Rest};
parse_topics(Num, <<Size:32/signed, Topic:Size/binary, NumPartitions:32/signed, Rest/binary>>, Topics) ->
    {Partitions, Rest1} = parse_partitions(NumPartitions, Rest),
    parse_topics(Num-1, Rest1, [{Topic, Partitions} | Topics]).

-spec parse_partitions(non_neg_integer(), binary()) -> {[partition()], binary()}.
parse_partitions(Num, Raw) ->
    parse_partitions(Num, Raw, []).

parse_partitions(0, Rest, Partitions) ->
    {Partitions, Rest};
parse_partitions(Num, <<Partition:32/signed, FetchOffset:64/signed, MaxBytes:32/signed, Rest/binary>>, Partitions) ->
    parse_partitions(Num-1, Rest, [{Partition, FetchOffset, MaxBytes} | Partitions]).

flush_socket(Socket) ->
    receive
        {tcp, Socket, Data}         -> flush_send(Socket, Data);
        {tcp_error, Socket, Reason} -> Reason;
        {tcp_closed, Socket}        -> normal
    after
        0                           -> normal
    end.

flush_send(Socket, Data) ->
    case gen_tcp:send(Socket, Data) of
        ok              -> flush_recv(Socket);
        {error, closed} -> normal;
        {error, Reason} -> Reason
    end.

flush_recv(Socket) ->
    case gen_tcp:recv(Socket, 0, 0) of
        {ok, Data}       -> flush_send(Socket, Data);
        {error, timeout} -> normal;
        {error, closed}  -> normal;
        {error, Reason}  -> Reason
end.

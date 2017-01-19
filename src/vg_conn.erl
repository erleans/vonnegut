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

-include("vg.hrl").

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
    lager:debug("at=new_connection node=~p peer=~p", [node(), _PeerName]),
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
    lager:info("Gracefully closing ~p~n", [Socket]),
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
handle_request(<<ApiKey:16/signed, _ApiVersion:16/signed, CorrelationId:32/signed,
                 ClientIdSize:32/signed, _ClientId:ClientIdSize/binary, Request/binary>>, Socket) ->
    More = handle_request(ApiKey, Request, CorrelationId, Socket),
    More.

handle_request(?FETCH_REQUEST, <<_ReplicaId:32/signed, _MaxWaitTime:32/signed,
                                 _MinBytes:32/signed, Rest/binary>>, CorrelationId, Socket) ->
    {[{Topic, [{Partition, Offset, _MaxBytes} | _]} | _], _} = vg_protocol:decode_array(fun decode_topic/1, Rest),
    {SegmentId, Position} = vg_log_segments:find_segment_offset(Topic, Partition, Offset),

    File = vg_utils:log_file(Topic, Partition, SegmentId),
    {ok, Fd} = file:open(File, [read, binary, raw]),
    try
        Bytes = filelib:file_size(File) - Position,
        gen_tcp:send(Socket, <<(Bytes + 4):32/signed, CorrelationId:32/signed>>),
        {ok, _} = file:sendfile(Fd, Socket, Position, Bytes, [])
    after
        file:close(Fd)
    end;
handle_request(?PRODUCE_REQUEST, Data, CorrelationId, Socket) ->
    {_Acks, _Timeout, TopicData} = vg_protocol:decode_produce_request(Data),
    Results = [{Topic, [begin
                            {ok, Offset} = vg_active_segment:write(Topic, Partition, MessageSet),
                            {Partition, ?NONE_ERROR, Offset}
                        end || {Partition, MessageSet} <- PartitionData]}
              || {Topic, PartitionData} <- TopicData],
    ProduceResponse = vg_protocol:encode_produce_response(Results),
    Size = erlang:iolist_size(ProduceResponse) + 4,
    gen_tcp:send(Socket, [<<Size:32/signed, CorrelationId:32/signed>>, ProduceResponse]);
handle_request(?TOPICS_REQUEST, <<>>, CorrelationId, Socket) ->
    Children = [Partition || {Partition, _, _, [C]} <- supervisor:which_children(vonnegut_sup),
                             C == vg_topic_sup],
    Topics = [vg_protocol:encode_string(T) || T <- Children],
    Response = vg_protocol:encode_array(Topics),
    Size = erlang:iolist_size(Response) + 4,
    gen_tcp:send(Socket, [<<Size:32/signed, CorrelationId:32/signed>>, Response]).

-spec decode_topic(binary()) -> {topic_partition(), binary()}.
decode_topic(<<Size:32/signed, Topic:Size/binary, PartitionsRaw/binary>>) ->
    {Partitions, Rest} = vg_protocol:decode_array(fun decode_partition/1, PartitionsRaw),
    {{Topic, Partitions}, Rest}.

-spec decode_partition(binary()) -> {partition(), binary()}.
decode_partition(<<Partition:32/signed, FetchOffset:64/signed, MaxBytes:32/signed, Rest/binary>>) ->
    {{Partition, FetchOffset, MaxBytes}, Rest}.

%%

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

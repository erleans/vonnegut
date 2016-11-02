-module(vg_client).

-behavior(shackle_client).

-export([
    init/0,
    setup/2,
    handle_request/2,
    handle_data/2,
    terminate/1
]).

-include("vg.hrl").

-record(state, {
          request_counter = 0    :: non_neg_integer(),
          buffer          = <<>> :: binary()
         }).

-spec init() -> {ok, term()}.
init() ->
     {ok, #state {}}.

-spec setup(inet:socket(), term()) -> {ok, term()} | {error, term(), term()}.
setup(_Socket, State) ->
    {ok, State}.

-spec handle_request(term(), term()) -> {ok, non_neg_integer(), iodata(), term()}.
handle_request({fetch, Topic, Partition}, #state {
        request_counter = RequestCounter
    } = State) ->

    RequestId = request_id(RequestCounter),
    ReplicaId = -1,
    MaxWaitTime = 5000,
    MinBytes = 100,
    Request = vg_protocol:encode_fetch(ReplicaId, MaxWaitTime, MinBytes, [{Topic, [{Partition, 0, 100}]}]),
    Data = vg_protocol:encode_request(?FETCH_REQUEST, RequestId, ?CLIENT_ID, Request),

    {ok, RequestId, [<<(iolist_size(Data)):32/signed>>, Data], State#state{request_counter = RequestCounter + 1}}.

-spec handle_data(binary(), term()) -> {ok, term(), term()}.
handle_data(Data, State=#state{buffer=Buffer}) ->
    Data2 = <<Buffer/binary, Data/binary>>,
    case vg_protocol:decode_fetch(Data2) of
        more ->
            {ok, [], State#state{buffer = <<Buffer/binary, Data/binary>>}};
        Result ->
            {ok, [Result], State#state{buffer = <<>>}}
    end.

-spec terminate(term()) -> ok.
terminate(_State) ->
    ok.

%% private
request_id(RequestCounter) ->
    RequestCounter rem ?MAX_REQUEST_ID.

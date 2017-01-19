-module(vg_client).

%%-behavior(shackle_client). ?

-export([fetch/1, fetch/2,
         fetch_until/2, fetch_until/3,
         produce/2,
         init/0,
         setup/2,
         handle_request/2,
         handle_data/2,
         terminate/1]).

-include("vg.hrl").

-record(state, {
          request_counter = 0    :: non_neg_integer(),
          corids          = #{}  :: maps:map(),
          buffer          = <<>> :: binary()
         }).

fetch(Topic) ->
    fetch(Topic, 0).

fetch(Topic, Position) ->
    {ok, Pool} = vg_client_pool:get_pool(Topic, read),
    lager:info("fetch request to pool: ~p ~p", [Topic, Pool]),
    shackle:call(Pool, {fetch, Topic, 0, Position}).

fetch_until(Topic, Target) ->
    fetch_until(Topic, 0, Target).

fetch_until(Topic, Position, Target) ->
    {ok, Pool} = vg_client_pool:get_pool(Topic, read),
    lager:info("fetch request to pool: ~p ~p", [Topic, Pool]),
    Loop =
        fun Loop(Acc=#{high_water_mark := Offset}) ->
                Resp0 = shackle:call(Pool, {fetch, Topic, 0, Offset}),
                Resp = merge_fetch_response(Acc, Resp0),
                #{high_water_mark := Mark} = Resp,
                case Mark >= Target of
                    true ->
                        Resp;
                    _ ->
                        Loop(Resp#{high_water_mark => Mark+1})
                end
        end,
    Loop(#{message_set => [], high_water_mark => Position}).

merge_fetch_response(One, Two) ->
    #{message_set := Set1} = One,
    #{message_set := Set2} = Two,
    %% we assume ordering here, so Two's mark will be larger than
    %% one's, and when we append we'll preserve ordering.
    Two#{message_set := lists:append(Set1, Set2)}.

produce(Topic, RecordSet) ->
    {ok, Pool} = vg_client_pool:get_pool(Topic, write),
    lager:info("produce request to pool: ~p ~p", [Topic, Pool]),
    shackle:call(Pool, {produce, Topic, 0, RecordSet}).

-spec init() -> {ok, term()}.
init() ->
    {ok, #state{}}.

-spec setup(inet:socket(), term()) -> {ok, term()} | {error, term(), term()}.
setup(_Socket, State) ->
    {ok, State}.

-spec handle_request(term(), term()) -> {ok, non_neg_integer(), iodata(), term()}.
handle_request({fetch, Topic, Partition, Position}, #state {
                 request_counter = RequestCounter,
                 corids = CorIds
                } = State) ->
    RequestId = request_id(RequestCounter),
    ReplicaId = -1,
    MaxWaitTime = 5000,
    MinBytes = 100,
    Request = vg_protocol:encode_fetch(ReplicaId, MaxWaitTime, MinBytes, [{Topic, [{Partition, Position, 100}]}]),
    Data = vg_protocol:encode_request(?FETCH_REQUEST, RequestId, ?CLIENT_ID, Request),

    {ok, RequestId, [<<(iolist_size(Data)):32/signed>>, Data],
     State#state{corids = maps:put(RequestId, ?FETCH_REQUEST, CorIds),
                 request_counter = RequestCounter + 1}};
handle_request({produce, Topic, Partition, Records}, #state {
                 request_counter = RequestCounter,
                 corids = CorIds
                } = State) ->

    RequestId = request_id(RequestCounter),
    Acks = 0,
    Timeout = 5000,
    TopicData = [{Topic, [{Partition, Records}]}],
    Request = vg_protocol:encode_produce(Acks, Timeout, TopicData),
    Data = vg_protocol:encode_request(?PRODUCE_REQUEST, RequestId, ?CLIENT_ID, Request),

    {ok, RequestId, [<<(iolist_size(Data)):32/signed>>, Data],
     State#state{corids = maps:put(RequestId, ?PRODUCE_REQUEST, CorIds),
                 request_counter = RequestCounter + 1}};
handle_request(topics, #state {
                          request_counter = RequestCounter,
                          corids = CorIds
                         } = State) ->
    RequestId = request_id(RequestCounter),
    Data = vg_protocol:encode_request(?TOPICS_REQUEST, RequestId, ?CLIENT_ID, <<>>),
    {ok, RequestId, [<<(iolist_size(Data)):32/signed>>, Data],
     State#state{corids = maps:put(RequestId, ?TOPICS_REQUEST, CorIds),
                 request_counter = RequestCounter + 1}}.

-spec handle_data(binary(), term()) -> {ok, term(), term()}.
handle_data(Data, State=#state{buffer=Buffer,
                               corids=CorIds}) ->
    Data2 = <<Buffer/binary, Data/binary>>,
    case vg_protocol:decode_response(Data2) of
        more ->
            {ok, [], State#state{buffer = <<Buffer/binary, Data/binary>>}};
        {CorrelationId, Response, Rest} ->
            Result = vg_protocol:decode_response(maps:get(CorrelationId, CorIds), Response),
            lager:info("cli result ~p ~p", [Response, Result]),
            {ok, [{CorrelationId, Result}], State#state{corids = maps:remove(CorrelationId, CorIds),
                                                        buffer = Rest}}
    end.

-spec terminate(term()) -> ok.
terminate(_State) ->
    ok.

%% private
request_id(RequestCounter) ->
    RequestCounter rem ?MAX_REQUEST_ID.

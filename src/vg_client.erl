-module(vg_client).

%%-behavior(shackle_client). ?

-export([metadata/0,
         ensure_topic/1,
         fetch/1, fetch/2,
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

-define(TIMEOUT, 5000).

-spec metadata() -> {ok, {Chains :: vg_cluster_mgr:chains_map(),
                          Topics :: vg_cluster_mgr:topics_map()}}.
metadata() ->
    shackle:call(metadata, {metadata, []}).

-spec ensure_topic(Topic :: vg:topic()) -> {ok, {Chains :: vg_cluster_mgr:chains_map(),
                                                 Topics :: vg_cluster_mgr:topics_map()}}.
ensure_topic(Topic) ->
    Result = shackle:call(metadata, {metadata, [Topic]}, ?TIMEOUT),
    vg_client_pool:refresh_topic_map(),
    Result.

-spec fetch(Topic)
           -> {ok, #{high_water_mark := integer(),
                     record_set_size := integer(),
                     error_code := integer(),
                     record_set := RecordSet}}
                  when Topic :: vg:topic() | [{vg:topic(), [{integer(), integer(), integer()}]}],
                       RecordSet :: vg:record_set().
fetch(Topic) when is_binary(Topic) ->
    fetch(Topic, 0).

fetch(Topic, Position) ->
    {ok, Pool} = vg_client_pool:get_pool(Topic, read),
    lager:debug("fetch request to pool: ~p ~p", [Topic, Pool]),
    case shackle:call(Pool, {fetch, [{Topic, [{0, Position, 100}]}]}, ?TIMEOUT) of
        {ok, #{Topic := #{0 := Result=#{error_code := 0}}}} ->
            {ok, Result};
        {ok, #{Topic := #{0 := #{error_code := ErrorCode}}}} ->
            {error, ErrorCode};
        {error, Reason} ->
            {error, Reason}
    end.

-spec produce(Topic, RecordSet)
             -> {ok, integer()} | {error, term()}
                    when Topic :: vg:topic(),
                         RecordSet :: vg:record_set().
produce(Topic, RecordSet) ->
    {ok, Pool} = vg_client_pool:get_pool(Topic, write),
    lager:debug("produce request to pool: ~p ~p", [Topic, Pool]),
    TopicRecords = [{Topic, [{0, RecordSet}]}],
    case shackle:call(Pool, {produce, TopicRecords}, ?TIMEOUT) of
        {ok, #{Topic := #{0 := #{error_code := 0,
                                 offset := Offset}}}} ->
            {ok, Offset};
        {ok, #{Topic := #{0 := #{error_code := ErrorCode}}}} ->
            {error, ErrorCode};
        {error, Reason} ->
            {error, Reason}
    end.

-spec init() -> {ok, term()}.
init() ->
    {ok, #state{}}.

-spec setup(inet:socket(), term()) -> {ok, term()} | {error, term(), term()}.
setup(_Socket, State) ->
    {ok, State}.

-spec handle_request(term(), term()) -> {ok, non_neg_integer(), iodata(), term()}.
handle_request({metadata, Topics}, #state {
                 request_counter = RequestCounter,
                 corids = CorIds
                } = State) ->
    RequestId = request_id(RequestCounter),
    Request = vg_protocol:encode_metadata_request(Topics),
    Data = vg_protocol:encode_request(?METADATA_REQUEST, RequestId, ?CLIENT_ID, Request),

    {ok, RequestId, [<<(iolist_size(Data)):32/signed-integer>>, Data],
     State#state{corids = maps:put(RequestId, ?METADATA_REQUEST, CorIds),
                 request_counter = RequestCounter + 1}};
handle_request({fetch, TopicOffsets}, #state {
                 request_counter = RequestCounter,
                 corids = CorIds
                } = State) ->
    RequestId = request_id(RequestCounter),
    ReplicaId = -1,
    MaxWaitTime = 5000,
    MinBytes = 100,
    Request = vg_protocol:encode_fetch(ReplicaId, MaxWaitTime, MinBytes, TopicOffsets),
    Data = vg_protocol:encode_request(?FETCH_REQUEST, RequestId, ?CLIENT_ID, Request),
    {ok, RequestId, [<<(iolist_size(Data)):32/signed-integer>>, Data],
     State#state{corids = maps:put(RequestId, ?FETCH_REQUEST, CorIds),
                 request_counter = RequestCounter + 1}};
handle_request({produce, TopicRecords}, #state {
                 request_counter = RequestCounter,
                 corids = CorIds
                } = State) ->

    RequestId = request_id(RequestCounter),
    Acks = 0,
    Timeout = 5000,
    Request = vg_protocol:encode_produce(Acks, Timeout, TopicRecords),
    Data = vg_protocol:encode_request(?PRODUCE_REQUEST, RequestId, ?CLIENT_ID, Request),

    {ok, RequestId, [<<(iolist_size(Data)):32/signed-integer>>, Data],
     State#state{corids = maps:put(RequestId, ?PRODUCE_REQUEST, CorIds),
                 request_counter = RequestCounter + 1}};
handle_request(topics, #state {
                          request_counter = RequestCounter,
                          corids = CorIds
                         } = State) ->
    RequestId = request_id(RequestCounter),
    Data = vg_protocol:encode_request(?TOPICS_REQUEST, RequestId, ?CLIENT_ID, <<>>),
    {ok, RequestId, [<<(iolist_size(Data)):32/signed-integer>>, Data],
     State#state{corids = maps:put(RequestId, ?TOPICS_REQUEST, CorIds),
                 request_counter = RequestCounter + 1}}.

-spec handle_data(binary(), term()) -> {ok, term(), term()}.
handle_data(Data, State=#state{buffer=Buffer}) ->
    Data2 = <<Buffer/binary, Data/binary>>,
    decode_data(Data2, [], State).

decode_data(<<>>, Replies, State) ->
    {ok, Replies, State};
decode_data(Data, Replies, State=#state{corids=CorIds}) ->
    case vg_protocol:decode_response(Data) of
        more ->
            {ok, Replies, State#state{buffer = Data}};
        {CorrelationId, Response, Rest} ->
            Result = vg_protocol:decode_response(maps:get(CorrelationId, CorIds), Response),
            decode_data(Rest, [{CorrelationId, {ok, Result}} | Replies],
                        State#state{corids = maps:remove(CorrelationId, CorIds),
                                    buffer = <<>>})
    end.

-spec terminate(term()) -> ok.
terminate(_State) ->
    ok.

%% private
request_id(RequestCounter) ->
    RequestCounter rem ?MAX_REQUEST_ID.

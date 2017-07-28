-module(vg_client).

%%-behavior(shackle_client). ?

-export([metadata/0, metadata/1,
         ensure_topic/1,
         topics/0, topics/2,
         fetch/1, fetch/2, fetch/3,
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
          buffer          = <<>> :: binary(),
          expected_size   = 0    :: non_neg_integer()
         }).

-define(TIMEOUT, 5000).

-spec metadata() -> {ok, {Chains :: vg_cluster_mgr:chains_map(),
                          Topics :: vg_cluster_mgr:topics_map()}}.
metadata() ->
    %% this is maybe a silly default, considering that it could return
    %% millions of topics
    metadata([]).

metadata(Topics) ->
    shackle:call(metadata, {metadata, Topics}).

-spec ensure_topic(Topic :: vg:topic()) ->
                          {ok, {Chains :: vg_cluster_mgr:chains_map(),
                                Topics :: vg_cluster_mgr:topics_map()}} |
                          {error, Reason :: term()}.
ensure_topic(Topic) ->
    %% always use the metadata topic, creation happens inside via a global process.
    shackle:call(metadata, {metadata, [Topic]}, ?TIMEOUT).


-spec fetch(Topic)
           -> {ok, #{high_water_mark := integer(),
                     record_set_size := integer(),
                     error_code := integer(),
                     record_set := RecordSet}}
                  when Topic :: vg:topic() | [{vg:topic(), [{integer(), integer(), integer()}]}],
                       RecordSet :: vg:record_set().

%% if we don't want to expose the tuple in the second clauses of
%% fetch/1 and fetch/2, we could do something like fetch_partial,
%% which would return the tuple and options, which then could be fed
%% into an execute_multifetch function which would do the right thing.

fetch(Topic) when is_binary(Topic) ->
    do_fetch([{Topic, 0, #{}}], ?TIMEOUT);
fetch(Requests) when is_list(Requests) ->
    do_fetch(Requests, ?TIMEOUT).

fetch(Topic, Position) when is_binary(Topic) ->
    do_fetch([{Topic, Position, #{}}], ?TIMEOUT);
fetch(Requests, Timeout) when is_list(Requests) ->
    do_fetch(Requests, Timeout).

fetch(Topic, Position, MaxIndex) when is_binary(Topic) ->
    do_fetch([{Topic, Position, #{max_index => MaxIndex}}], ?TIMEOUT).

do_fetch(Requests, Timeout) ->
    PoolReqs =
        lists:foldl(
          fun({Topic, _Position, _Opts} = R, Acc) ->
                  {ok, Pool} = vg_client_pool:get_pool(Topic, read),
                  lager:debug("fetch request to pool: ~p ~p", [Topic, Pool]),
                  case Acc of
                      #{Pool := PoolReqs} ->
                          Acc#{Pool => [R | PoolReqs]};
                      _ ->
                          Acc#{Pool => [R]}
                  end
          end, #{}, Requests),
    %% should we do these in parallel?
    Resps = maps:map(
              fun(Pool, TPO0) ->
                      TPO = [begin
                                 MaxBytes = maps:get(max_bytes, Opts, 0),
                                 MaxIndex = maps:get(max_index, Opts, -1),
                                 {Topic, [{0, Position, MaxBytes, MaxIndex}]}
                             end
                             || {Topic, Position, Opts} <- TPO0],
                      case shackle:call(Pool, {fetch2, TPO}, Timeout) of
                          {ok, Result} ->
                              %% if there are any error codes in any
                              %% of these, transform the whole thing
                              %% into an error
                              {ok, Result};
                          {error, Reason} ->
                              {error, Reason}
                      end
              end, PoolReqs),
    lists:foldl(
      fun(_, {error, Response}) ->
              {error, Response};
         ({_Pool, {ok, Response}}, {ok, Acc}) ->
              {ok, maps:merge(Acc, Response)};
         ({_Pool, {error, Response}}, _) ->
              {error, Response}
      end,
      {ok, #{}}, maps:to_list(Resps)).

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

topics() ->
    topics(metadata, []).

topics(Pool, Topic) ->
    case shackle:call(Pool,  {topics, Topic}, ?TIMEOUT) of
        {ok, {_, _}} = OK ->
            OK;
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
handle_request({Fetch, TopicOffsets}, #state {
                 request_counter = RequestCounter,
                 corids = CorIds
                } = State) when Fetch == fetch orelse Fetch == fetch2 ->
    RequestId = request_id(RequestCounter),
    ReplicaId = -1,
    MaxWaitTime = 5000,
    MinBytes = 100,
    ReqType = case Fetch of fetch -> ?FETCH_REQUEST; fetch2 -> ?FETCH2_REQUEST end,
    Request = vg_protocol:encode_fetch(ReplicaId, MaxWaitTime, MinBytes, TopicOffsets),
    Data = vg_protocol:encode_request(ReqType, RequestId, ?CLIENT_ID, Request),
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
handle_request({topics, Topics}, #state {
                 request_counter = RequestCounter,
                 corids = CorIds
                } = State) ->

    RequestId = request_id(RequestCounter),
    Request = vg_protocol:encode_array([<<(byte_size(T)):16/signed-integer,
                                          T/binary>> || T <- Topics]),
    Data = vg_protocol:encode_request(?TOPICS_REQUEST, RequestId, ?CLIENT_ID, Request),

    {ok, RequestId, [<<(iolist_size(Data)):32/signed-integer>>, Data],
     State#state{corids = maps:put(RequestId, ?TOPICS_REQUEST, CorIds),
                 request_counter = RequestCounter + 1}}.

-spec handle_data(binary(), term()) -> {ok, term(), term()}.
handle_data(Data, State=#state{buffer=Buffer}) ->
    Data2 = <<Buffer/binary, Data/binary>>,
    decode_data(Data2, [], State).

decode_data(<<>>, Replies, State) ->
    {ok, Replies, State};
decode_data(Data, Replies, State=#state{corids=CorIds, expected_size = Exp}) ->
    case Exp of
        N when N == 0 orelse byte_size(Data) >= N ->
            case vg_protocol:decode_response(Data) of
                more ->
                    {ok, Replies, State#state{buffer = Data}};
                {more, Size} ->
                    {ok, Replies, State#state{buffer = Data, expected_size = Size}};
                {CorrelationId, Response, Rest} ->
                    Result = vg_protocol:decode_response(maps:get(CorrelationId, CorIds), Response),
                    decode_data(Rest, [{CorrelationId, {ok, Result}} | Replies],
                                State#state{corids = maps:remove(CorrelationId, CorIds),
                                            expected_size = 0,
                                            buffer = <<>>})
            end;
        _ ->
            {ok, Replies, State#state{buffer = Data}}
    end.

-spec terminate(term()) -> ok.
terminate(_State) ->
    ok.

%% private
request_id(RequestCounter) ->
    RequestCounter rem ?MAX_REQUEST_ID.

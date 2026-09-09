-module(vg_client).

-compile([warn_missing_spec_all, warn_untyped_record]).

-behavior(shackle_client).

-export([metadata/0, metadata/1,
         ensure_topic/1,
         delete_topic/1,
         topics/0, topics/2,
         fetch/1, fetch/2, fetch/3,

         %% internal-only stuff
         replicate/5,
         delete_topic/2,
         %% end internal

         produce/2, produce/3,
         init/1,
         setup/2,
         handle_request/2,
         handle_data/2,
         terminate/1]).

-include("vg.hrl").

-type fetch_options() :: #{max_bytes => non_neg_integer(), limit => integer()}.
-type fetch_requests() :: [{vg:topic(), integer(), fetch_options()}].
-type fetch_result() :: {ok, vg_protocol:fetch_response()} | {error, term()}.
-export_type([fetch_options/0, fetch_requests/0]).

-record(state, {
          request_counter = 0    :: non_neg_integer(),
          buffer          = <<>> :: binary(),
          expected_size   = 0    :: non_neg_integer()
         }).

-define(TIMEOUT, 5000).

-spec metadata() -> {ok, vg_protocol:metadata_response()} | {error, term()}.
metadata() ->
    %% this is maybe a silly default, considering that it could return
    %% millions of topics
    metadata([]).

-spec metadata([vg:topic()]) -> {ok, vg_protocol:metadata_response()} | {error, term()}.
metadata(Topics) ->
    Request = vg_protocol:encode_metadata_request(Topics),
    scall(metadata, ?METADATA_REQUEST, Request, ?TIMEOUT, fun vg_protocol:decode_metadata_response/1).

-spec ensure_topic(vg:topic()) -> {ok, vg_protocol:metadata_response()} | {error, term()}.
ensure_topic(Topic) ->
    %% always use the metadata topic, creation happens inside via a global process.
    Request = vg_protocol:encode_metadata_request([Topic]),
    scall(metadata, ?ENSURE_REQUEST, Request, ?TIMEOUT, fun vg_protocol:decode_metadata_response/1).

%% if we don't want to expose the tuple in the second clauses of
%% fetch/1 and fetch/2, we could do something like fetch_partial,
%% which would return the tuple and options, which then could be fed
%% into an execute_multifetch function which would do the right thing.

-spec fetch(vg:topic() | fetch_requests()) -> fetch_result().
fetch(Topic) when is_binary(Topic) ->
    do_fetch([{Topic, 0, #{}}], ?TIMEOUT);
fetch(Requests) when is_list(Requests) ->
    do_fetch(Requests, ?TIMEOUT).

-spec fetch(vg:topic() | fetch_requests(), integer()) -> fetch_result().
fetch(Topic, Position) when is_binary(Topic) ->
    do_fetch([{Topic, Position, #{}}], ?TIMEOUT);
fetch(Requests, Timeout) when is_list(Requests) ->
    do_fetch(Requests, Timeout).

-spec fetch(vg:topic(), integer(), integer()) -> fetch_result().
fetch(Topic, Position, Limit) when is_binary(Topic) ->
    do_fetch([{Topic, Position, #{limit => Limit}}], ?TIMEOUT).

-spec do_fetch(fetch_requests(), integer()) -> fetch_result().
do_fetch(Requests, Timeout) ->
    try
        PoolReqs =
            lists:foldl(
              fun({Topic, _Position, _Opts} = R, Acc) ->
                      case vg_client_pool:get_pool(Topic, read) of
                          {ok, Pool} ->
                              logger:debug("fetch request to pool: ~p ~p", [Topic, Pool]),
                              case Acc of
                                  #{Pool := PoolReqs} ->
                                      Acc#{Pool => [R | PoolReqs]};
                                  _ ->
                                      Acc#{Pool => [R]}
                              end;
                          {error, Reason} ->
                              throw({error, Reason})
                      end
              end, #{}, Requests),
        %% should we do these in parallel?
        Restart = application:get_env(vonnegut, swap_restart, true),
        Resps = maps:map(
                  fun(Pool, TPO0) ->
                          TPO = [begin
                                     MaxBytes = maps:get(max_bytes, Opts, 0),
                                     Limit = maps:get(limit, Opts, -1),
                                     {Topic, [{0, Position, MaxBytes, Limit}]}
                                 end
                                 || {Topic, Position, Opts} <- TPO0],
                          ReplicaId = -1,
                          MaxWaitTime = 5000,
                          MinBytes = 100,
                          Request = vg_protocol:encode_fetch(ReplicaId, MaxWaitTime, MinBytes, TPO),
                          case scall(Pool, ?FETCH2_REQUEST, Request, Timeout, fun vg_protocol:decode_fetch_response/1) of
                              %% sometimes because of cloud orchestration, and
                              %% restarts, head and tail nodes will switch or
                              %% move around in time for us to reconnect to them
                              %% in error, so if we get these codes, start over
                              {ok, Map} when is_map(Map) andalso Restart =:= true ->
                                  case maps:fold(
                                         fun(_, _, true) ->
                                                 true;
                                            (_, #{0 := #{error_code := ?FETCH_DISALLOWED_ERROR}}, _) ->
                                                 true;
                                            (T, #{0 := #{error_code := ?UNKNOWN_TOPIC_OR_PARTITION}}, _) ->
                                                 throw({error, {T, not_found}});
                                            (_, _, _) ->
                                                 false
                                         end, false, Map) of
                                      true ->
                                          throw(restart);
                                      _ ->
                                          {ok, Map}
                                  end;
                              {ok, more} ->
                                  {error, incomplete_response};
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
          {ok, #{}}, maps:to_list(Resps))
    catch throw:{error, {Topic, not_found}} ->
            logger:error("tried to fetch from non-existent topic ~p", [Topic]),
            {error, {Topic, not_found}};
          throw:restart ->
            logger:info("disallowed request error, restarting pools"),
            vg_client_pool:restart(),
            do_fetch(Requests, Timeout)
    end.

-spec replicate(atom(), vg:topic(), integer(), iodata(), integer()) -> {ok, integer()} | {error, term()} | {write_repair, [vg:record_batch()]} | retry.
replicate(Pool, Topic, ExpectedId, RecordBatch, Timeout) ->
    logger:debug("replicate pool=~p topic=~p", [Pool, Topic]),
    Request = vg_protocol:encode_replicate(0, 5000, Topic, 0, ExpectedId, RecordBatch),
    case scall(Pool, ?REPLICATE_REQUEST, Request, Timeout, fun vg_protocol:decode_replicate_response/1) of
        {ok, {0, #{error_code := 0,
                   offset := Offset}}} ->
            {ok, Offset};
        {ok, {0, #{error_code := ?WRITE_REPAIR, records := RecordBatches}}} ->
            {write_repair, RecordBatches};
        {ok, {0, #{error_code := ?TIMEOUT_ERROR}}} ->
            retry;
        {ok, {0, #{error_code := ErrorCode}}} ->
            {error, ErrorCode};
        {error, Reason} ->
            {error, Reason}
    end.

-spec delete_topic(vg:topic()) -> ok | {error, term()}.
delete_topic(Topic) ->
    case vg_client_pool:get_pool(Topic, write) of
        {ok, Pool} ->
            Request = vg_protocol:encode_delete_topic(Topic),
            case scall(Pool, ?DELETE_TOPIC_REQUEST, Request, timer:seconds(60), fun vg_protocol:decode_delete_topic_response/1) of
                {ok, ok} -> ok;
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec delete_topic(atom(), vg:topic()) -> ok | {error, term()}.
delete_topic(Pool, Topic) ->
    logger:debug("delete_topic pool=~p topic=~p", [Pool, Topic]),
    Request = vg_protocol:encode_delete_topic(Topic),
    case scall(Pool, ?REPLICATE_DELETE_TOPIC_REQUEST, Request, timer:seconds(60), fun vg_protocol:decode_delete_topic_response/1) of
        {ok, ok} -> ok;
        {error, Reason} -> {error, Reason}
    end.

-spec produce(vg:topic(), vg:batch_input()) -> {ok, integer()} | {error, term()}.
produce(Topic, RecordBatch) ->
    produce(Topic, RecordBatch, ?TIMEOUT).

-spec produce(vg:topic(), vg:batch_input(), pos_integer()) -> {ok, integer()} | {error, term()}.
produce(Topic, RecordBatch, Timeout) ->
    #{record_batch := EncodedRecordBatch} = vg_protocol:encode_record_batch(RecordBatch),
    produce_(Topic, EncodedRecordBatch, Timeout).

-spec produce_(vg:topic(), iodata(), pos_integer()) -> {ok, integer()} | {error, term()}.
produce_(Topic, EncodedRecordBatch, Timeout) ->
    case vg_client_pool:get_pool(Topic, write) of
        {ok, Pool} ->
            logger:debug("produce request to pool: ~p ~p", [Topic, Pool]),
            TopicRecords = [{Topic, [{0, EncodedRecordBatch}]}],
            Restart = application:get_env(vonnegut, swap_restart, true),
            Request = vg_protocol:encode_produce(0, 5000, TopicRecords),
            case scall(Pool, ?PRODUCE_REQUEST, Request, Timeout, fun vg_protocol:decode_produce_response/1) of
                {ok, #{Topic := #{0 := #{error_code := 0,
                                         offset := Offset}}}} ->
                    {ok, Offset};
                {ok, #{Topic := #{0 := #{error_code := ?TIMEOUT_ERROR}}}} ->
                    {error, timeout};
                %% sometimes because of cloud orchestration, and
                %% restarts, head and tail nodes will switch or
                %% move around in time for us to reconnect to them
                %% in error, so if we get these codes, start over
                {ok, #{Topic := #{0 := #{error_code := ?PRODUCE_DISALLOWED_ERROR}}}}
                  when Restart =:= true ->
                    logger:info("disallowed request error, restarting pools"),
                    vg_client_pool:restart(),
                    produce_(Topic, EncodedRecordBatch, Timeout);
                {ok, #{Topic := #{0 := #{error_code := ErrorCode}}}} ->
                    {error, ErrorCode};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

-spec topics() -> {ok, {integer(), [vg_protocol:wire_chain()]}} | {error, term()}.
topics() ->
    topics(metadata, []).

-spec topics(atom(), [vg:topic()]) -> {ok, {integer(), [vg_protocol:wire_chain()]}} | {error, term()}.
topics(Pool, Topics) ->
    Request = vg_protocol:encode_array([<<(byte_size(T)):16/signed-integer, T/binary>> || T <- Topics]),
    case scall(Pool, ?TOPICS_REQUEST, Request, ?TIMEOUT, fun vg_protocol:decode_topics_response/1) of
        {ok, {_, _}} = OK ->
            OK;
        {error, Reason} ->
            {error, Reason}
    end.

-spec init(term()) -> {ok, #state{}}.
init(_) ->
    {ok, #state{}}.

-spec setup(inet:socket(), #state{}) -> {ok, #state{}}.
setup(_Socket, State) ->
    {ok, State}.

-spec handle_request({integer(), iodata()}, #state{}) -> {ok, integer(), iodata(), #state{}}.
handle_request({Type, Body}, State=#state{request_counter=RequestCounter}) ->
    Id = request_id(RequestCounter),
    Data = vg_protocol:encode_request(Type, Id, ?CLIENT_ID, Body),
    {ok, Id, [<<(iolist_size(Data)):32/signed-integer>>, Data],
     State#state{request_counter = RequestCounter + 1}}.

-spec handle_data(binary(), #state{}) -> {ok, [{integer(), {ok, binary()}}], #state{}}.
handle_data(Data, State=#state{buffer=Buffer}) ->
    Data2 = <<Buffer/binary, Data/binary>>,
    decode_data(Data2, [], State).

-spec decode_data(binary(), [{integer(), {ok, binary()}}], #state{}) -> {ok, [{integer(), {ok, binary()}}], #state{}}.
decode_data(<<>>, Replies, State) ->
    {ok, Replies, State};
decode_data(Data, Replies, State=#state{expected_size=Exp}) ->
    case Exp of
        N when N == 0 orelse byte_size(Data) >= N ->
            case vg_protocol:decode_response(Data) of
                more ->
                    {ok, Replies, State#state{buffer=Data}};
                {more, Size} ->
                    {ok, Replies, State#state{buffer=Data, expected_size=Size}};
                {CorrelationId, Response, Rest} ->
                    decode_data(Rest, [{CorrelationId, {ok, Response}} | Replies],
                                State#state{expected_size = 0,
                                            buffer = <<>>})
            end;
        _ ->
            {ok, Replies, State#state{buffer=Data}}
    end.

-spec terminate(#state{}) -> ok.
terminate(_State) ->
    ok.

%% private
-spec request_id(non_neg_integer()) -> non_neg_integer().
request_id(RequestCounter) ->
    RequestCounter rem ?MAX_REQUEST_ID.

-spec scall(atom(), integer(), iodata(), timeout(), fun((binary()) -> T)) -> {ok, T} | {error, term()}.
scall(Pool, RequestType, RequestData, RequestTimeout, Decode) ->
    B = backoff:init(2, 200),
    B1 = backoff:type(B, jitter),
    %% at these settings, 25 retries is approximately 5s
    scall(Pool, RequestType, RequestData, RequestTimeout, Decode, B1, 25).

-spec scall(atom(), integer(), iodata(), timeout(), fun((binary()) -> T), backoff:backoff(), non_neg_integer()) -> {ok, T} | {error, term()}.
scall(_, _, _, _, _, _, 0) ->
    {error, pool_timeout};
scall(Pool, RequestType, RequestData, RequestTimeout, Decode, Backoff, Retries) ->
    case shackle:call(Pool,  {RequestType, RequestData}, RequestTimeout) of
        {error, timeout} ->
            {error, timeout};
        {error, _} ->
            {Time, Backoff1} = backoff:fail(Backoff),
            timer:sleep(Time),
            scall(Pool, RequestType, RequestData, RequestTimeout, Decode, Backoff1, Retries - 1);
        {ok, Response} when is_binary(Response) ->
            {ok, Decode(Response)}
    end.

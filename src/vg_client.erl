-module(vg_client).

-behavior(shackle_client).

-export([metadata/0, metadata/1,
         ensure_topic/1,
         topics/0, topics/2,
         fetch/1, fetch/2, fetch/3,

         %% internal-only stuff
         replicate/5,
         delete_topic/2,
         %% end internal

         produce/2, produce/3,
         init/0,
         setup/2,
         handle_request/2,
         handle_data/2,
         terminate/1]).

-include("vg.hrl").

-record(state, {
          request_counter = 0    :: non_neg_integer(),
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
    Request = vg_protocol:encode_metadata_request(Topics),
    scall(metadata, ?METADATA_REQUEST, Request, ?TIMEOUT).

-spec ensure_topic(Topic :: vg:topic()) ->
                          {ok, {Chains :: vg_cluster_mgr:chains_map(),
                                Topics :: vg_cluster_mgr:topics_map()}} |
                          {error, Reason :: term()}.
ensure_topic(Topic) ->
    %% always use the metadata topic, creation happens inside via a global process.
    Request = vg_protocol:encode_metadata_request([Topic]),
    scall(metadata, ?ENSURE_REQUEST, Request, ?TIMEOUT).

-spec fetch(Topic)
           -> {ok, #{high_water_mark := integer(),
                     record_batches_size := integer(),
                     error_code := integer(),
                     record_batches := RecordBatches}}
                  when Topic :: vg:topic() | [{vg:topic(), [{integer(), integer(), integer()}]}],
                       RecordBatches :: [vg:record_batch()].

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

fetch(Topic, Position, Limit) when is_binary(Topic) ->
    do_fetch([{Topic, Position, #{limit => Limit}}], ?TIMEOUT).

do_fetch(Requests, Timeout) ->
    try
        PoolReqs =
            lists:foldl(
              fun({Topic, _Position, _Opts} = R, Acc) ->
                      case vg_client_pool:get_pool(Topic, read) of
                          {ok, Pool} ->
                              lager:debug("fetch request to pool: ~p ~p", [Topic, Pool]),
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
                          case scall(Pool, ?FETCH2_REQUEST, Request, Timeout) of
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
            lager:error("tried to fetch from non-existent topic ~p", [Topic]),
            {error, {Topic, not_found}};
          throw:restart ->
            lager:info("disallowed request error, restarting pools"),
            vg_client_pool:restart(),
            do_fetch(Requests, Timeout)
    end.

-spec replicate(Pool, Topic, ExpectedId, RecordBatch, Timeout)
             -> {ok, integer()} | {error, term()} | {write_repair, maps:map()} | retry
                    when Pool :: atom(),
                         Topic :: vg:topic(),
                         ExpectedId :: integer(),
                         RecordBatch :: vg:record_batch() | [vg:record_batch()],
                         Timeout :: integer().
replicate(Pool, Topic, ExpectedId, RecordBatch, Timeout) ->
    lager:debug("replicate pool=~p topic=~p", [Pool, Topic]),
    Request = vg_protocol:encode_replicate(0, 5000, Topic, 0, ExpectedId, RecordBatch),
    case scall(Pool, ?REPLICATE_REQUEST, Request, Timeout) of
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

delete_topic(Pool, Topic) ->
    lager:debug("delete_topic pool=~p topic=~p", [Pool, Topic]),
    Request = vg_protocol:encode_delete_topic(Topic),
    case scall(Pool, ?DELETE_TOPIC_REQUEST, Request, timer:seconds(60)) of
        {ok, ok} -> ok;
        {error, Reason} -> {error, Reason}
    end.

-spec produce(Topic, RecordBatch)
             -> {ok, integer()} | {error, term()}
                    when Topic :: vg:topic(),
                         RecordBatch :: vg:record_batch() | [vg:record_batch()].
produce(Topic, RecordBatch) ->
    produce(Topic, RecordBatch, ?TIMEOUT).

-spec produce(Topic, RecordBatch, Timeout)
             -> {ok, integer()} | {error, term()}
                    when Topic :: vg:topic(),
                         RecordBatch :: vg:record_batch() | [vg:record_batch()],
                         Timeout :: pos_integer().
produce(Topic, RecordBatch, Timeout) ->
    #{record_batch := EncodedRecordBatch} = vg_protocol:encode_record_batch(RecordBatch),
    produce_(Topic, EncodedRecordBatch, Timeout).

produce_(Topic, EncodedRecordBatch, Timeout) ->
    case vg_client_pool:get_pool(Topic, write) of
        {ok, Pool} ->
            lager:debug("produce request to pool: ~p ~p", [Topic, Pool]),
            TopicRecords = [{Topic, [{0, EncodedRecordBatch}]}],
            Restart = application:get_env(vonnegut, swap_restart, true),
            Request = vg_protocol:encode_produce(0, 5000, TopicRecords),
            case scall(Pool, ?PRODUCE_REQUEST, Request, Timeout) of
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
                    lager:info("disallowed request error, restarting pools"),
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

topics() ->
    topics(metadata, []).

topics(Pool, Topics) ->
    Request = vg_protocol:encode_array([<<(byte_size(T)):16/signed-integer, T/binary>> || T <- Topics]),
    case scall(Pool, ?TOPICS_REQUEST, Request, ?TIMEOUT) of
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

-spec handle_request({integer(), iodata()}, #state{}) -> {ok, non_neg_integer(), iodata(), term()}.
handle_request({Type, Body}, State=#state{request_counter=RequestCounter}) ->
    Id = request_id(RequestCounter),
    Data = vg_protocol:encode_request(Type, Id, ?CLIENT_ID, Body),
    {ok, Id, [<<(iolist_size(Data)):32/signed-integer>>, Data],
     State#state{request_counter = RequestCounter + 1}}.

-spec handle_data(binary(), term()) -> {ok, [{term(),term()}], term()}.
handle_data(Data, State=#state{buffer=Buffer}) ->
    Data2 = <<Buffer/binary, Data/binary>>,
    decode_data(Data2, [], State).

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

-spec terminate(term()) -> ok.
terminate(_State) ->
    ok.

%% private
request_id(RequestCounter) ->
    RequestCounter rem ?MAX_REQUEST_ID.

scall(Pool, RequestType, RequestData, RequestTimeout) ->
    B = backoff:init(2, 200),
    B1 = backoff:type(B, jitter),
    %% at these settings, 25 retries is approximately 5s
    scall(Pool, RequestType, RequestData, RequestTimeout, B1, 25).

scall(_, _, _, _, _, 0) ->
    {error, pool_timeout};
scall(Pool, RequestType, RequestData, RequestTimeout, Backoff, Retries) ->
    case shackle:call(Pool,  {RequestType, RequestData}, RequestTimeout) of
        {error, no_socket} ->
            {Time, Backoff1} = backoff:fail(Backoff),
            timer:sleep(Time),
            scall(Pool, RequestType, RequestData, RequestTimeout, Backoff1, Retries - 1);
        {error, socket_closed} ->
            {Time, Backoff1} = backoff:fail(Backoff),
            timer:sleep(Time),
            scall(Pool, RequestType, RequestData, RequestTimeout, Backoff1, Retries - 1);
        {error, timeout} ->
            {error, timeout};
        {ok, Response} ->
            {ok, vg_protocol:decode_response(RequestType, Response)}
    end.

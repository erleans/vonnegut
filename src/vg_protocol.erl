-module(vg_protocol).

-compile([warn_missing_spec_all, warn_untyped_record]).

-export([decode_metadata_response/1, decode_replicate_response/1,
         decode_delete_topic_response/1, decode_produce_response/1, decode_topics_response/1,
         encode_fetch/4,
         encode_produce/3,
         encode_record_batch/1,
         encode_replicate/6,
         encode_delete_topic/1,
         encode_request/4,
         encode_metadata_response/2,
         encode_metadata_request/1,
         encode_chains/1,

         encode_array/1,
         encode_kv/1,
         encode_string/1,
         encode_bytes/1,
         encode_produce_response/1,
         encode_replicate_response/1,
         encode_fetch_topic_response/4,
         decode_array/2,

         decode_metadata_request/1,
         decode_string/1,
         last_offset_delta/1,

         decode_produce_request/1,
         decode_replicate_request/1,
         decode_fetch_response/1,
         decode_response/1,
         decode_response/2,
         decode_record_batches/1]).

-include("vg.hrl").

-type fetch_partition() :: {vg:partition(), integer(), non_neg_integer()} |
                           {vg:partition(), integer(), non_neg_integer(), integer()}.
-type fetch_topics() :: [{vg:topic(), [fetch_partition()]}].
-type produce_data() :: [{vg:topic(), [{vg:partition(), iodata()}]}].
-type produce_request() :: [{vg:topic(), [{vg:partition(), [vg:record_batch()]}]}].
-type produce_partitions() :: [{vg:partition(), integer(), integer()}].
-type produce_results() :: [{vg:topic(), produce_partitions()}].
-type produce_partition_response() :: #{error_code := integer(), offset := integer()}.
-type produce_response() :: #{vg:topic() => #{vg:partition() => produce_partition_response()}}.
-type fetch_partition_response() :: #{high_water_mark := integer(),
                                      record_batches_size := non_neg_integer(),
                                      error_code := integer(),
                                      record_batches := [vg:log_record()]}.
-type fetch_response() :: #{vg:topic() => #{vg:partition() => fetch_partition_response()}}.
-type chain_boundary() :: binary() | start_space | end_space.
-type wire_chain() :: #{name := binary(), topics_start := chain_boundary(),
                       topics_end := chain_boundary(),
                       head := {binary(), inet:port_number()},
                       tail := {binary(), inet:port_number()}}.
-type decoded_broker() :: {integer(), {binary(), inet:port_number()}}.
-type broker() :: {integer(), {binary() | string(), inet:port_number()}}.
-type partition_metadata() :: {integer(), integer(), integer(), [integer()], [integer()]}.
-type topic_metadata() :: {integer(), vg:topic(), [partition_metadata()]}.
-type decoded_partition_metadata() :: #{partition_id := integer(), leader := integer(),
                                        isrs := [integer()], replicas := [integer()]}.
-type decoded_topic_metadata() :: #{topic := vg:topic(), partitions := [decoded_partition_metadata()]}.
-type metadata_response() :: {vg_cluster_mgr:chains_map(), vg_cluster_mgr:topics_map()}.
-type replicate_response() :: {vg:partition(), #{error_code := integer(), offset := integer()}} |
                              {vg:partition(), #{error_code := 133, records := [vg:record_batch()]}}.
-type response() :: fetch_response() | produce_response() | replicate_response() |
                    metadata_response() | {integer(), [wire_chain()]} | ok | {error, binary()} | more.
-export_type([fetch_partition/0, fetch_topics/0, fetch_response/0, metadata_response/0,
              wire_chain/0, chain_boundary/0, response/0]).

%% encode requests

-spec encode_fetch(integer(), integer(), integer(), fetch_topics()) -> iodata().
encode_fetch(ReplicaId, MaxWaitTime, MinBytes, Topics) ->
    [<<ReplicaId:32/signed-integer, MaxWaitTime:32/signed-integer, MinBytes:32/signed-integer>>, encode_topics(Topics)].

-spec encode_produce(-1 | 0 | 1, integer(), produce_data()) -> iodata().
encode_produce(Acks, Timeout, TopicData) when Acks =:= -1
                                              ; Acks =:= 0
                                              ; Acks =:= 1 ->
    [<<Acks:16/signed-integer, Timeout:32/signed-integer>>, encode_topic_data(TopicData)].

-spec encode_delete_topic(vg:topic()) -> iodata().
encode_delete_topic(Topic) ->
    encode_string(Topic).

-spec encode_replicate(-1 | 0 | 1, integer(), vg:topic(), vg:partition(), integer(), iodata()) -> iodata().
encode_replicate(Acks, Timeout, Topic, Partition, ExpectedId, Data) when Acks =:= -1
                                                                         ; Acks =:= 0
                                                                         ; Acks =:= 1 ->
    [<<Acks:16/signed-integer, Timeout:32/signed-integer>>, encode_replicate_topic_data(Topic, Partition, ExpectedId, Data)].

-spec encode_replicate_topic_data(vg:topic(), vg:partition(), integer(), iodata()) -> iodata().
encode_replicate_topic_data(Topic, Partition, ExpectedId, RecordBatch) ->
    [encode_string(Topic), <<Partition:32/signed-integer, ExpectedId:64/signed-integer,
                             (iolist_size(RecordBatch)):32/signed-integer>>, RecordBatch].

-spec encode_topic_data(produce_data()) -> iodata().
encode_topic_data(TopicData) ->
    encode_array([[encode_string(Topic), encode_data(Data)] || {Topic, Data} <- TopicData]).

-spec encode_data([{vg:partition(), iodata()}]) -> iodata().
encode_data(Data) ->
    encode_array([begin
                      [<<Partition:32/signed-integer, (iolist_size(RecordBatch)):32/signed-integer>>, RecordBatch]
                  end || {Partition, RecordBatch} <- Data]).

-spec encode_topics(fetch_topics()) -> iodata().
encode_topics(Topics) ->
    encode_array([[encode_string(Topic), encode_partitions(Partitions)] || {Topic, Partitions} <- Topics]).

-spec encode_partitions([fetch_partition()]) -> iodata().
encode_partitions(Partitions) ->
    encode_array([case PartitionTuple of
                      {Partition, Offset, MaxBytes} ->
                          <<Partition:32/signed-integer, Offset:64/signed-integer, MaxBytes:32/signed-integer>>;
                      {Partition, Offset, MaxBytes, Limit} ->
                          <<Partition:32/signed-integer, Offset:64/signed-integer,
                            MaxBytes:32/signed-integer, Limit:32/signed-integer>>
                  end || PartitionTuple <- Partitions]).

-spec encode_request(integer(), integer(), binary() | string(), iodata()) -> iodata().
encode_request(ApiKey, CorrelationId, ClientId, Request) ->
    [<<ApiKey:16/signed-integer, ?API_VERSION:16/signed-integer, CorrelationId:32/signed-integer>>,
     encode_string(ClientId), Request].

-spec encode_metadata_request([vg:topic()]) -> iodata().
encode_metadata_request(Topics) ->
    encode_array([encode_string(Topic) || Topic <- Topics]).

-spec encode_chains([chain()]) -> iodata().
encode_chains(Chains) ->
    encode_array([begin
                      Start = maybe_convert(Start0),
                      End = maybe_convert(End0),
                      <<HeadPort:16/unsigned-integer,
                        TailPort:16/unsigned-integer,
                        (iolist_to_binary(
                           encode_array([encode_string(to_binary(Name)),
                                         encode_string(Start),
                                         encode_string(End),
                                         encode_string(HeadHost),
                                         encode_string(TailHost)])))/binary>>
                  end
                  || #chain{name = Name,
                            nodes = _Nodes,
                            topics_start = Start0,
                            topics_end = End0,
                            head = {HeadHost, HeadPort},
                            tail = {TailHost, TailPort}} <- Chains]).

%% generic encode functions

-spec to_binary(binary() | atom()) -> binary().
to_binary(Bin) when is_binary(Bin) ->
    Bin;
to_binary(Atm) when is_atom(Atm) ->
    atom_to_binary(Atm, utf8).

-spec encode_string(undefined | binary() | string()) -> iodata().
encode_string(undefined) ->
    <<-1:16/signed-integer>>;
encode_string(Data) when is_binary(Data) ->
    [<<(size(Data)):16/signed-integer>>, Data];
encode_string(Data) ->
    [<<(length(Data)):16/signed-integer>>, Data].

-spec encode_array([iodata()]) -> iodata().
encode_array(Array) ->
    [<<(length(Array)):32/signed-integer>>, Array].

-spec encode_bytes(undefined | binary()) -> iodata().
encode_bytes(undefined) ->
    <<-1:32/signed-integer>>;
encode_bytes(Data) ->
    [<<(size(Data)):32/signed-integer>>, Data].

-spec record_timestamp_or_now(binary() | vg:log_record()) -> integer().
record_timestamp_or_now(#{timestamp := Timestamp}) when is_integer(Timestamp) ->
    Timestamp;
record_timestamp_or_now(_) ->
    erlang:system_time(millisecond).

-spec encode_record_batch(vg:batch_input()) -> vg:record_batch() | [].
encode_record_batch(#{producer_id := ProducerId,
                      producer_epoch := ProducerEpoch,
                      sequence_number := FirstSequenceNumber,
                      records := Records}) ->
    encode_record_batch(ProducerId, ProducerEpoch, FirstSequenceNumber, Records);
encode_record_batch(#{records := Records}) ->
    encode_record_batch(0, 0, 0, Records);
encode_record_batch(Records) ->
    encode_record_batch(0, 0, 0, Records).

-spec encode_record_batch(integer(), integer(), integer(), vg:records()) -> vg:record_batch() | [].
encode_record_batch(_, _, _, []) ->
    [];
encode_record_batch(ProducerId, ProducerEpoch, FirstSequenceNumber, Record) when is_binary(Record) ->
    encode_record_batch(ProducerId, ProducerEpoch, FirstSequenceNumber, [Record]);
encode_record_batch(ProducerId, ProducerEpoch, FirstSequenceNumber, Records=[F | _]) ->
    FirstTimestamp = record_timestamp_or_now(F),
    {EncodedRecords, MaxTimestamp, LastOffsetDelta}
        = lists:foldl(fun(Record=#{timestamp := Timestamp}, {EncodedAcc, MaxTimestamp, Counter}) ->
                              TimestampDelta = FirstTimestamp - Timestamp,
                              {[encode_record(Record, TimestampDelta, Counter) | EncodedAcc], max(Timestamp, MaxTimestamp), Counter+1};
                         (Record, {EncodedAcc, MaxTimestamp, Counter}) ->
                              {[encode_record(Record, 0, Counter) | EncodedAcc], MaxTimestamp, Counter+1}
                      end, {[], FirstTimestamp, 0}, Records),

    RecordBatch = [<<0:16/signed-integer, %% Attributes: lowest 3 bits are for compression.
                     %% fourth lowest bit is 0 for timestamp being for record create time
                     %% 5 and 6 are for transactions and control messages
                     (LastOffsetDelta-1):32/signed-integer, %% LastOffsetDelta
                     FirstTimestamp:64/signed-integer, %% FirstTimestamp
                     MaxTimestamp:64/signed-integer, %% MaxTimestamp
                     ProducerId:64/signed-integer, %% ProducerId
                     ProducerEpoch:16/signed-integer, %% ProducerEpoch
                     FirstSequenceNumber:32/signed-integer>>, %% FirstSequence
                   EncodedRecords],
    Crc = erlang:crc32(RecordBatch),
    RecordBatch1 = [<<0:32/signed-integer, %% PartitionLeaderEpoch
                      ?MAGIC_TWO:8/signed-integer,
                      Crc:32/signed-integer>>,
                    RecordBatch],
    Size = iolist_size(RecordBatch1),
    #{last_offset_delta => (LastOffsetDelta-1),
      crc => Crc,
      size => Size,
      record_batch => RecordBatch1}.

-spec headers(binary() | vg:log_record()) -> [{binary(), binary()}].
headers(#{headers := Headers}) when is_list(Headers) ->
    Headers;
headers(_) ->
    [].

-spec encode_header({binary(), binary()}) -> iodata().
encode_header({Key, Value}) ->
    [encode_varint(size(Key)), Key, encode_varint(size(Value)), Value].

-spec encode_record(binary() | vg:log_record(), integer(), integer()) -> iodata().
encode_record(Record, TimestampDelta, OffsetDelta) ->
    Headers = headers(Record),
    %% no key
    EncodedValue = case Record of
                       #{value := Value} ->
                           Key = maps:get(key, Record, <<>>),
                           [encode_varint(size(Key)), Key, encode_varint(size(Value)), Value];
                       _ ->
                           [encode_varint(0), encode_varint(size(Record)), Record]
                   end,
    EncodedRecord = [<<0:8/signed-integer>>, %% Attributes: currently unused by Kafka
                     encode_varint(TimestampDelta), %% TimestampDelta:
                     encode_varint(OffsetDelta),
                     EncodedValue,
                     encode_array([encode_header(H) || H <- Headers])], %% Headers: ordered and can contain duplicates

    [encode_varint(iolist_size(EncodedRecord)) | EncodedRecord].

-spec encode_varint(non_neg_integer()) -> binary().
encode_varint(I) when is_integer(I), I >= 0, I =< 127 ->
    <<I>>;
encode_varint(I) when is_integer(I), I > 127 ->
    <<1:1, (I band 127):7, (encode_varint(I bsr 7))/binary>>;
encode_varint(I) when is_integer(I), I < 0 ->
    erlang:error({badarg, I}).

-spec encode_kv(binary() | {binary() | undefined, binary() | undefined}) -> iodata().
encode_kv({Key, Value}) ->
    [encode_bytes(Key), encode_bytes(Value)];
encode_kv(Value) ->
    [encode_bytes(undefined), encode_bytes(Value)].

%% encode responses

-spec encode_produce_response(produce_results()) -> iodata().
encode_produce_response(TopicPartitions) ->
    encode_produce_response(TopicPartitions, []).

-spec encode_produce_response(produce_results(), [iodata()]) -> iodata().
encode_produce_response([], Acc) ->
    encode_array(Acc);
encode_produce_response([{Topic, Partitions} | T], Acc) ->
    encode_produce_response(T, [[encode_string(Topic), encode_produce_response_partitions(Partitions)] | Acc]).

-spec encode_produce_response_partitions(produce_partitions()) -> iodata().
encode_produce_response_partitions(Partitions) ->
    encode_array([<<Partition:32/signed-integer, ErrorCode:16/signed-integer, Offset:64/signed-integer>>
                      || {Partition, ErrorCode, Offset} <- Partitions]).

-spec encode_replicate_response({vg:partition(), integer(), integer() | [{integer(), binary()}] | [vg:record_batch()]}) -> iodata().
encode_replicate_response({Partition, ?WRITE_REPAIR, RecordBatch}) when is_list(RecordBatch) ->
    Records = [encode_repair_batch(Batch) || Batch <- RecordBatch],
    [<<Partition:32/signed-integer, ?WRITE_REPAIR:16/signed-integer, (iolist_size(Records)):32/signed-integer>>, Records];
encode_replicate_response({Partition, ErrorCode, Offset}) when is_integer(Offset) ->
    <<Partition:32/signed-integer, ErrorCode:16/signed-integer, Offset:64/signed-integer>>.

-spec encode_fetch_topic_response(vg:partition(), integer(), integer(), non_neg_integer()) -> binary().
encode_fetch_topic_response(Partition, ErrorCode, HighWaterMark, Bytes) ->
    <<Partition:32/signed-integer, ErrorCode:16/signed-integer, HighWaterMark:64/signed-integer, Bytes:32/signed-integer>>.

-spec encode_metadata_response([broker()], [topic_metadata()]) -> iodata().
encode_metadata_response(Brokers, TopicMetadata) ->
    [encode_brokers(Brokers), encode_topic_metadata(TopicMetadata)].

-spec encode_brokers([broker()]) -> iodata().
encode_brokers(Brokers) ->
    encode_array([[<<NodeId:32/signed-integer>>, encode_string(Host), <<Port:32/signed-integer>>]
                 || {NodeId, {Host, Port}} <- Brokers]).

-spec encode_topic_metadata([topic_metadata()]) -> iodata().
encode_topic_metadata(TopicMetadata) ->
    encode_array([[<<TopicErrorCode:16/signed-integer>>, encode_string(Topic), encode_partition_metadata(PartitionMetadata)]
                    || {TopicErrorCode, Topic, PartitionMetadata} <- TopicMetadata]).

-spec encode_partition_metadata([partition_metadata()]) -> iodata().
encode_partition_metadata(PartitionMetadata) ->
    encode_array([[<<PartitionErrorCode:16/signed-integer, PartitionId:32/signed-integer, Leader:32/signed-integer>>,
                   encode_array([<<Replica:32/signed-integer>> || Replica <- Replicas]),
                   encode_array([<<I:32/signed-integer>> || I <- Isr])]
                  || {PartitionErrorCode, PartitionId, Leader, Replicas, Isr} <- PartitionMetadata]).

%% decode generic functions

-spec decode_array(fun((binary()) -> {T, binary()} | more), binary()) -> {[T], binary()} | more.
decode_array(DecodeFun, <<Length:32/signed-integer, Rest/binary>>) ->
    decode_array(DecodeFun, Length, Rest, []);
decode_array(_, _) ->
    more.

-spec decode_array(fun((binary()) -> {T, binary()} | more), integer(), binary(), [T]) -> {[T], binary()} | more.
decode_array(_, 0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
decode_array(DecodeFun, N, Rest, Acc) ->
    case DecodeFun(Rest) of
        {Element, Rest1} ->
            decode_array(DecodeFun, N-1, Rest1, [Element | Acc]);
        more -> more
    end.

-spec decode_int32(binary()) -> {integer(), binary()}.
decode_int32(<<I:32/signed-integer, Rest/binary>>) ->
    {I, Rest}.

-spec decode_string(binary()) -> {binary(), binary()}.
decode_string(<<Size:16/signed-integer, String:Size/bytes, Rest/binary>>) ->
    {String, Rest}.

-spec last_offset_delta(binary()) -> integer().
last_offset_delta(<<_LeaderEpoch:32/signed-integer, ?MAGIC_TWO:8/signed-integer,
                    _CRC:32/signed-integer, _Attributes:16/signed-integer, LastOffsetDelta:32/signed-integer, _Rest/binary>>) ->
    LastOffsetDelta;
last_offset_delta(_) ->
    0.

-spec partial_decode_record_batch(binary()) -> [vg:record_batch()].
partial_decode_record_batch(<<>>) ->
    [];
partial_decode_record_batch(RecordBatch = <<_LeaderEpoch:32/signed-integer, ?MAGIC_TWO:8/signed-integer, CRC:32/signed-integer,
                                    _Attributes:16/signed-integer, LastOffsetDelta:32/signed-integer, _/binary>>) ->
    [#{crc => CRC,
       size => size(RecordBatch),
       last_offset_delta => LastOffsetDelta,
       record_batch => RecordBatch}].

%% unlike a produce, a replicate may send more than 1 batch
-spec partial_decode_record_batches(binary()) -> [vg:record_batch()].
partial_decode_record_batches(<<>>) ->
    [];
partial_decode_record_batches(<<Id:64/signed-integer, Length:32/signed-integer, RecordBatch:Length/binary, Rest/binary>>) ->
    case RecordBatch of
        <<_LeaderEpoch:32/signed-integer, ?MAGIC_TWO:8/signed-integer, CRC:32/signed-integer,
          _Attributes:16/signed-integer, LastOffsetDelta:32/signed-integer, _/binary>> ->
            [#{crc => CRC,
               base_offset => Id,
               size => Length,
               last_offset_delta => LastOffsetDelta,
               record_batch => RecordBatch} | partial_decode_record_batches(Rest)];
        _ ->
            []
    end.

%% decode requests

-spec decode_metadata_request(binary()) -> [vg:topic()].
decode_metadata_request(Msg) ->
    {Topics, _rest} = decode_array(fun decode_topics/1, Msg),
    Topics.

-spec decode_topics(binary()) -> {vg:topic(), binary()}.
decode_topics(<<Size:16/signed-integer, Topic:Size/bytes, Rest/binary>>) ->
    {Topic, Rest}.

-spec decode_produce_request(binary()) -> {integer(), integer(), produce_request()}.
decode_produce_request(<<Acks:16/signed-integer, Timeout:32/signed-integer, Topics/binary>>) ->
    {TopicsDecode, _} = decode_array(fun decode_produce_topics_request/1, Topics),
    {Acks, Timeout, TopicsDecode}.

-spec decode_produce_topics_request(binary()) -> {{vg:topic(), [{vg:partition(), [vg:record_batch()]}]}, binary()}.
decode_produce_topics_request(<<Size:16/signed-integer, Topic:Size/binary, Rest/binary>>) ->
    {TopicData, Rest1} = decode_array(fun decode_produce_partitions_request/1, Rest),
    {{Topic, TopicData}, Rest1}.

-spec decode_produce_partitions_request(binary()) -> {{vg:partition(), [vg:record_batch()]}, binary()}.
decode_produce_partitions_request(<<Partition:32/signed-integer, Size:32/signed-integer,
                                    RecordBatch:Size/binary, Rest/binary>>) ->
    {{Partition, partial_decode_record_batch(RecordBatch)}, Rest}.

%% decode replicate

-spec decode_replicate_request(binary()) -> {integer(), integer(), {vg:topic(), vg:partition(), integer(), [vg:record_batch()]}}.
decode_replicate_request(<<Acks:16/signed-integer, Timeout:32/signed-integer, TopicSize:16/signed-integer,
                           Topic:TopicSize/binary, Partition:32/signed-integer, ExpectedId:64/signed-integer,
                           RecordBatchSize:32/signed-integer, RecordBatch:RecordBatchSize/binary, _Rest/binary>>) ->
    {Acks, Timeout, {Topic, Partition, ExpectedId, partial_decode_record_batch(RecordBatch)}}.

%% decode responses

-spec decode_response(binary()) -> {integer(), binary(), binary()} | {more, integer()} | more.
decode_response(<<Size:32/signed-integer, Record:Size/binary, Rest/binary>>) ->
    <<CorrelationId:32/signed-integer, Response/binary>> = Record,
    {CorrelationId, Response, Rest};
decode_response(<<Size:32/signed-integer, Rest/binary>>) ->
    {more, Size - byte_size(Rest)};
decode_response(_) ->
    more.

-spec decode_response(integer(), binary()) -> response().
decode_response(?FETCH_REQUEST, Response) ->
    decode_fetch_response(Response);
decode_response(?FETCH2_REQUEST, Response) ->
    decode_fetch_response(Response);
decode_response(?PRODUCE_REQUEST, Response) ->
    decode_produce_response(Response);
decode_response(?REPLICATE_REQUEST, Response) ->
    decode_replicate_response(Response);
decode_response(?DELETE_TOPIC_REQUEST, Response) ->
    decode_delete_topic_response(Response);
decode_response(?REPLICATE_DELETE_TOPIC_REQUEST, Response) ->
    decode_delete_topic_response(Response);
decode_response(?TOPICS_REQUEST, Response) ->
    decode_topics_response(Response);
decode_response(?METADATA_REQUEST, Response) ->
    decode_metadata_response(Response);
decode_response(?ENSURE_REQUEST, Response) ->
    decode_metadata_response(Response).

-spec decode_metadata_response(binary()) -> metadata_response().
decode_metadata_response(Response) ->
    {Brokers, Rest} = decode_array(fun decode_metadata_response_brokers/1, Response),
    {TopicMetadata, _}= decode_array(fun decode_metadata_response_topic_metadata/1, Rest),

    %% The response will use leader as the head node and the first element in isrs as the tail
    %% We use the head host node as the name of the chain
    {Chains, Topics} = lists:foldl(fun(#{topic := Topic,
                                         partitions := [#{leader := HeadId,
                                                          isrs := [TailId | _]} | _]}, Acc) ->
                                       update_chains_and_topics(Brokers, Topic, HeadId, TailId, Acc)
                                   end, {#{}, #{}}, TopicMetadata),

    {Chains, Topics}.

-spec update_chains_and_topics([decoded_broker()], vg:topic(), integer(), integer(), metadata_response()) -> metadata_response().
update_chains_and_topics(Brokers, Topic, HeadId, TailId, {ChainsAcc, TopicsAcc}) ->
    {_, {HeadHost, HeadPort}} = lists:keyfind(HeadId, 1, Brokers),
    {_, {TailHost, TailPort}} = lists:keyfind(TailId, 1, Brokers),
    ChainName = <<HeadHost/binary, "-", (integer_to_binary(HeadPort))/binary>>,
    {maps:put(ChainName, #chain{name = ChainName,
                                head = {binary_to_list(HeadHost), HeadPort},
                                tail = {binary_to_list(TailHost), TailPort}}, ChainsAcc),
     maps:put(Topic, ChainName, TopicsAcc)}.


-spec decode_metadata_response_brokers(binary()) -> {decoded_broker(), binary()}.
decode_metadata_response_brokers(<<NodeId:32/signed-integer, HostSize:16/signed-integer,
                                   Host:HostSize/binary, Port:32/signed-integer, Rest/binary>>) ->
    {{NodeId, {binary:copy(Host), Port}}, Rest}.

-spec decode_metadata_response_topic_metadata(binary()) -> {decoded_topic_metadata(), binary()}.
decode_metadata_response_topic_metadata(<<_ErrorCode:16/signed-integer, TopicSize:16/signed-integer,
                                          Topic:TopicSize/binary, PartitionMetadataRest/binary>>) ->
    {PartitionMetadata, Rest} = decode_array(fun decode_partition_metadata/1, PartitionMetadataRest),
    {#{topic => binary:copy(Topic),
       partitions => PartitionMetadata}, Rest}.

-spec decode_partition_metadata(binary()) -> {decoded_partition_metadata(), binary()}.
decode_partition_metadata(<<_PartitionErrorCode:16/signed-integer, PartitionId:32/signed-integer,
                            Leader:32/signed-integer, Rest/binary>>) ->
    {Isrs, Rest1}= decode_array(fun decode_int32/1, Rest),
    {Replicas, Rest2}= decode_array(fun decode_int32/1, Rest1),

    {#{partition_id => PartitionId,
       leader => Leader,
       isrs => Isrs,
       replicas => Replicas}, Rest2}.

-spec decode_produce_response(binary()) -> produce_response() | more.
decode_produce_response(Response) ->
    case decode_array(fun decode_produce_response_topics/1, Response) of
        {TopicResults, _Rest} ->
            maps:from_list(TopicResults);
        more -> more
    end.

-spec decode_produce_response_topics(binary()) -> {{vg:topic(), #{vg:partition() => produce_partition_response()}}, binary()} | more.
decode_produce_response_topics(<<Size:16/signed-integer, Topic:Size/binary, PartitionsRaw/binary>>) ->
    case decode_array(fun decode_produce_response_partitions/1, PartitionsRaw) of
        {Partitions, Rest} ->
            {{Topic, maps:from_list(Partitions)}, Rest};
        more -> more
    end;
decode_produce_response_topics(_) ->
    more.

-spec decode_produce_response_partitions(binary()) -> {{vg:partition(), produce_partition_response()}, binary()} | more.
decode_produce_response_partitions(<<Partition:32/signed-integer, ErrorCode:16/signed-integer,
                                     Offset:64/signed-integer, Rest/binary>>) ->
    {{Partition, #{error_code => ErrorCode,
                   offset => Offset}}, Rest};
decode_produce_response_partitions(_) ->
    more.

-spec decode_replicate_response(binary()) -> replicate_response() | more.
decode_replicate_response(<<Partition:32/signed-integer, ?WRITE_REPAIR:16/signed-integer,
                            Size:32/signed-integer, RecordBatch:Size/binary, _Rest/binary>>) ->
    {Partition, #{error_code => ?WRITE_REPAIR, records => partial_decode_record_batches(RecordBatch)}};
decode_replicate_response(<<Partition:32/signed-integer, ErrorCode:16/signed-integer,
                            Offset:64/signed-integer, _Rest/binary>>) ->
    {Partition, #{error_code => ErrorCode, offset => Offset}};
decode_replicate_response(_) ->
    more.

-spec decode_delete_topic_response(binary()) -> ok | {error, binary()}.
decode_delete_topic_response(Data) ->
    case decode_string(Data) of
        {<<"OK">>, <<>>} -> ok;
        {Reason, <<>>} -> {error, Reason}
    end.

-spec decode_topics_response(binary()) -> {integer(), [wire_chain()]}.
decode_topics_response(<<TopicsCount:32/signed-integer, Msg/binary>>) ->
    {Chains, _Rest} = decode_array(fun decode_chain/1, Msg),
    {TopicsCount, Chains}.

-spec decode_chain(binary()) -> {wire_chain(), binary()}.
decode_chain(<<HeadPort:16/unsigned-integer,
               TailPort:16/unsigned-integer,
               Array/binary>>) ->
    {[Name, Start0, End0, HeadHost, TailHost], Rest} = decode_array(fun decode_string/1, Array),
    Start = maybe_deconvert(Start0),
    End = maybe_deconvert(End0),
    {#{name => Name,
       topics_start => Start,
       topics_end => End,
       head => {HeadHost, HeadPort},
       tail => {TailHost, TailPort}},
     Rest}.

-spec maybe_convert(chain_boundary() | undefined) -> binary() | undefined.
maybe_convert(undefined) ->
    undefined;
maybe_convert(start_space) ->
    <<"$$start_space">>;
maybe_convert(end_space) ->
    <<"$$end_space">>;
maybe_convert(B) when is_binary(B) ->
    B.

%% split these to unconfuse dialyzer
-spec maybe_deconvert(binary()) -> chain_boundary().
maybe_deconvert(<<"$$start_space">>) ->
    start_space;
maybe_deconvert(<<"$$end_space">>) ->
    end_space;
maybe_deconvert(B) when is_binary(B) ->
    B.

-spec decode_fetch_response(binary()) -> fetch_response() | more.
decode_fetch_response(Msg) ->
    case decode_array(fun decode_fetch_topic/1, Msg) of
        {Topics, _Rest} ->
            maps:from_list(Topics);
        more -> more
    end.

-spec decode_fetch_topic(binary()) -> {{vg:topic(), #{vg:partition() => fetch_partition_response()}}, binary()} | more.
decode_fetch_topic(<<Sz:16/signed-integer, Topic:Sz/binary, Partitions/binary>>) ->
    case decode_array(fun decode_fetch_partition/1, Partitions) of
        {DecPartitions, Rest} ->
            {{Topic, maps:from_list(DecPartitions)}, Rest};
        more ->
            more
    end;
decode_fetch_topic(_) ->
    more.

-spec decode_fetch_partition(binary()) -> {{vg:partition(), fetch_partition_response()}, binary()} | more.
decode_fetch_partition(<<Partition:32/signed-integer, ErrorCode:16/signed-integer,
                         HighWaterMark:64/signed-integer, Bytes:32/signed-integer, RecordBatch:Bytes/bytes,
                         Rest/binary>>) ->
    %% there are a number of fields that we're not including currently:
    %% - throttle time
    %% - topic name
    {{Partition, #{high_water_mark => HighWaterMark,
                   record_batches_size => Bytes,
                   error_code => ErrorCode,
                   record_batches => decode_record_batches(RecordBatch)}}, Rest};
decode_fetch_partition(_) ->
    more.

%%
-spec decode_record_batches(binary() | eof) -> [vg:log_record()].
decode_record_batches(RecordBatches) ->
    decode_record_batches(RecordBatches, []).

%% TODO: validate recordsize
-spec decode_record_batches(binary() | eof, [vg:log_record()]) -> [vg:log_record()].
decode_record_batches(End, Set) when End =:= eof orelse End =:= <<>> ->
    lists:reverse(Set);
decode_record_batches(<<FirstOffset:64/signed-integer, Length:32/signed-integer, LeaderEpoch:32/signed-integer,
                    ?MAGIC_TWO:8/signed-integer, Rest/binary>>, Set) ->
    {Set1, Rest0} = decode_record_batch(FirstOffset, <<Length:32/signed-integer, LeaderEpoch:32/signed-integer,
                                                       ?MAGIC_TWO:8/signed-integer, Rest/binary>>, Set),
    decode_record_batches(Rest0, Set1);
decode_record_batches(_, Set) ->
    Set.

-spec decode_record_batch(integer(), binary(), [vg:log_record()]) -> {[vg:log_record()], binary()}.
decode_record_batch(FirstOffset, <<Length:32/signed-integer, Batch:Length/binary, Rest/binary>>, Set) ->
    <<_LeaderEpoch:32/signed-integer,
      ?MAGIC_TWO:8/signed-integer,
      _CRC:32/signed-integer,
      Attributes:16/signed-integer,
      _LastOffsetDelta:32/signed-integer,
      FirstTimestamp:64/signed-integer,
      _MaxTimestamp:64/signed-integer,
      _ProducerId:64/signed-integer,
      _ProducerEpoch:16/signed-integer,
      FirstSequenceNumber:32/signed-integer,
      Data/binary>> = Batch,
    case ?COMPRESSION(Attributes) of
        ?COMPRESS_NONE ->
            {decode_records(FirstOffset, FirstSequenceNumber, FirstTimestamp, Data, Set), Rest};
        ?COMPRESS_GZIP ->
            {decode_records(FirstOffset, FirstSequenceNumber, FirstTimestamp, zlib:gunzip(Data), Set), Rest};
        ?COMPRESS_SNAPPY ->
            {decode_records(FirstOffset, FirstSequenceNumber, FirstTimestamp, decompress(snappyer, decompress, Data), Set), Rest};
        ?COMPRESS_LZ4 ->
            {decode_records(FirstOffset, FirstSequenceNumber, FirstTimestamp, decompress(lz4, unpack, Data), Set), Rest}
    end;
decode_record_batch(FirstOffset, <<_Length:32/signed-integer, Rest/binary>>, Set) ->
    %% didn't get the whole batch, parse out as many as we can, if it is uncompresed
    case Rest of
        <<_LeaderEpoch:32/signed-integer,
          ?MAGIC_TWO:8/signed-integer,
          _CRC:32/signed-integer,
          Attributes:16/signed-integer,
          _LastOffsetDelta:32/signed-integer,
          FirstTimestamp:64/signed-integer,
          _MaxTimestamp:64/signed-integer,
          _ProducerId:64/signed-integer,
          _ProducerEpoch:16/signed-integer,
          FirstSequenceNumber:32/signed-integer,
          Data/binary>> ->
            case ?COMPRESSION(Attributes) of
                ?COMPRESS_NONE ->
                    {decode_records(FirstOffset, FirstSequenceNumber, FirstTimestamp, Data, Set), <<>>};
                _ ->
                    %% won't be able to uncompress
                    {Set, <<>>}
            end;
        _ ->
            %% not even all the metadata is here, return nothing
            {Set, <<>>}
    end.

-spec decode_records(integer(), integer(), integer(), binary(), [vg:log_record()]) -> [vg:log_record()].
decode_records(_FirstOffset, _FirstSequenceNumber, _FirstTimestamp, <<>>, Records) ->
    Records;
decode_records(FirstOffset, FirstSequenceNumber, FirstTimestamp, Data, Records) ->
    try
        {Length, Rest0} = decode_varint(Data, 0, 0),
        case Rest0 of
            <<RecordData:Length/binary, RestRecords/binary>> ->
                <<_Attributes:8/signed-integer, Rest1/binary>> = RecordData,
                {TimestampDelta, Rest2} = decode_varint(Rest1, 0, 0),
                {OffsetDelta, Rest3} = decode_varint(Rest2, 0, 0),
                {KeyLen, Rest4} = decode_varint(Rest3, 0, 0),
                <<Key:KeyLen/binary, Rest5/binary>> = Rest4,
                {ValueLen, Rest6} = decode_varint(Rest5, 0, 0),
                <<Value:ValueLen/binary, Rest7/binary>> = Rest6,
                {Headers, _Rest8} = decode_headers(Rest7),
                decode_records(FirstOffset, FirstSequenceNumber, FirstTimestamp,
                               RestRecords, [#{offset => OffsetDelta+FirstOffset,
                                               sequence_number => OffsetDelta+FirstSequenceNumber,
                                               timestamp => TimestampDelta+FirstTimestamp,
                                               key => Key,
                                               value => Value,
                                               headers => Headers} | Records]);
            _ ->
                %% didn't get the full record, return what we got so far
                Records
        end
    catch
        _:_ ->
            %% didn't even get the full lenght varint for this one, return what we got so far
            Records
    end.

-spec decode_varint(binary(), non_neg_integer(), non_neg_integer()) -> {non_neg_integer(), binary()}.
decode_varint(<<1:1, Number:7, Rest/binary>>, Position, Acc) ->
    decode_varint(Rest, Position + 7, (Number bsl Position) + Acc);
decode_varint(<<0:1, Number:7, Rest/binary>>, Position, Acc) ->
    {(Number bsl Position) + Acc, Rest}.

-spec decode_headers(binary()) -> {[{binary(), binary()}], binary()} | more.
decode_headers(Headers) ->
    decode_array(fun decode_header/1, Headers).

-spec decode_header(binary()) -> {{binary(), binary()}, binary()}.
decode_header(Header) ->
    {KeyLength, Rest0} = decode_varint(Header, 0, 0),
    <<Key:KeyLength/binary, Rest1/binary>> = Rest0,
    {ValueLength, Rest2} = decode_varint(Rest1, 0, 0),
    <<Value:ValueLength/binary, Rest3/binary>> = Rest2,
    {{Key, Value}, Rest3}.

%% Codecs are optional runtime integrations. Validate their output at the boundary;
%% snappyer returns {ok, Binary}, whereas lz4 returns the binary directly.
-spec decompress(module(), atom(), binary()) -> binary().
decompress(Module, Function, Data) ->
    case apply(Module, Function, [Data]) of
        {ok, Decoded} when is_binary(Decoded) -> Decoded;
        Decoded when is_binary(Decoded) -> Decoded;
        {error, Reason} -> error({decompression_failed, Module, Reason})
    end.

-spec encode_repair_batch({integer(), binary()} | vg:record_batch()) -> iodata().
encode_repair_batch({_, Data}) -> Data;
encode_repair_batch(#{base_offset := Offset, size := Size, record_batch := Data}) ->
    [<<Offset:64/signed-integer, Size:32/signed-integer>>, Data].

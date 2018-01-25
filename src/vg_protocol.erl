-module(vg_protocol).

-export([encode_fetch/4,
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

%% encode requests

encode_fetch(ReplicaId, MaxWaitTime, MinBytes, Topics) ->
    [<<ReplicaId:32/signed-integer, MaxWaitTime:32/signed-integer, MinBytes:32/signed-integer>>, encode_topics(Topics)].

encode_produce(Acks, Timeout, TopicData) when Acks =:= -1
                                              ; Acks =:= 0
                                              ; Acks =:= 1 ->
    [<<Acks:16/signed-integer, Timeout:32/signed-integer>>, encode_topic_data(TopicData)].

encode_delete_topic(Topic) ->
    encode_string(Topic).

encode_replicate(Acks, Timeout, Topic, Partition, ExpectedId, Data) when Acks =:= -1
                                                                         ; Acks =:= 0
                                                                         ; Acks =:= 1 ->
    [<<Acks:16/signed-integer, Timeout:32/signed-integer>>, encode_replicate_topic_data(Topic, Partition, ExpectedId, Data)].

encode_replicate_topic_data(Topic, Partition, ExpectedId, RecordBatch) ->
    [encode_string(Topic), <<Partition:32/signed-integer, ExpectedId:64/signed-integer,
                             (iolist_size(RecordBatch)):32/signed-integer>>, RecordBatch].

encode_topic_data(TopicData) ->
    encode_array([[encode_string(Topic), encode_data(Data)] || {Topic, Data} <- TopicData]).

encode_data(Data) ->
    encode_array([begin
                      [<<Partition:32/signed-integer, (iolist_size(RecordBatch)):32/signed-integer>>, RecordBatch]
                  end || {Partition, RecordBatch} <- Data]).

encode_topics(Topics) ->
    encode_array([[encode_string(Topic), encode_partitions(Partitions)] || {Topic, Partitions} <- Topics]).

encode_partitions(Partitions) ->
    encode_array([case PartitionTuple of
                      {Partition, Offset, MaxBytes} ->
                          <<Partition:32/signed-integer, Offset:64/signed-integer, MaxBytes:32/signed-integer>>;
                      {Partition, Offset, MaxBytes, Limit} ->
                          <<Partition:32/signed-integer, Offset:64/signed-integer,
                            MaxBytes:32/signed-integer, Limit:32/signed-integer>>
                  end || PartitionTuple <- Partitions]).

encode_request(ApiKey, CorrelationId, ClientId, Request) ->
    [<<ApiKey:16/signed-integer, ?API_VERSION:16/signed-integer, CorrelationId:32/signed-integer>>,
     encode_string(ClientId), Request].

encode_metadata_request(Topics) ->
    encode_array([encode_string(Topic) || Topic <- Topics]).

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

to_binary(Bin) when is_binary(Bin) ->
    Bin;
to_binary(Atm) when is_atom(Atm) ->
    atom_to_binary(Atm, utf8).

encode_string(undefined) ->
    <<-1:16/signed-integer>>;
encode_string(Data) when is_binary(Data) ->
    [<<(size(Data)):16/signed-integer>>, Data];
encode_string(Data) ->
    [<<(length(Data)):16/signed-integer>>, Data].

encode_array(Array) ->
    [<<(length(Array)):32/signed-integer>>, Array].

encode_bytes(undefined) ->
    <<-1:32/signed-integer>>;
encode_bytes(Data) ->
    [<<(size(Data)):32/signed-integer>>, Data].

record_timestamp_or_now(#{timestamp := Timestamp}) when is_integer(Timestamp) ->
    Timestamp;
record_timestamp_or_now(_) ->
    erlang:system_time(millisecond).

encode_record_batch(#{producer_id := ProducerId,
                      producer_epoch := ProducerEpoch,
                      sequence_number := FirstSequenceNumber,
                      records := Records}) ->
    encode_record_batch(ProducerId, ProducerEpoch, FirstSequenceNumber, Records);
encode_record_batch(#{records := Records}) ->
    encode_record_batch(0, 0, 0, Records);
encode_record_batch(Records) ->
    encode_record_batch(0, 0, 0, Records).

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

headers(#{headers := Headers}) when is_list(Headers) ->
    Headers;
headers(_) ->
    [].

encode_header({Key, Value}) ->
    [encode_varint(size(Key)), Key, encode_varint(size(Value)), Value].

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

encode_kv({Key, Value}) ->
    [encode_bytes(Key), encode_bytes(Value)];
encode_kv(Value) ->
    [encode_bytes(undefined), encode_bytes(Value)].

%% encode responses

encode_produce_response(TopicPartitions) ->
    encode_produce_response(TopicPartitions, []).

encode_produce_response([], Acc) ->
    encode_array(Acc);
encode_produce_response([{Topic, Partitions} | T], Acc) ->
    encode_produce_response(T, [[encode_string(Topic), encode_produce_response_partitions(Partitions)] | Acc]).

encode_produce_response_partitions(Partitions) ->
    encode_array([<<Partition:32/signed-integer, ErrorCode:16/signed-integer, Offset:64/signed-integer>>
                      || {Partition, ErrorCode, Offset} <- Partitions]).

encode_replicate_response({Partition, ?WRITE_REPAIR, RecordBatch}) ->
    Records = [Record || {_, Record} <- RecordBatch],
    [<<Partition:32/signed-integer, ?WRITE_REPAIR:16/signed-integer, (iolist_size(Records)):32/signed-integer>>, Records];
encode_replicate_response({Partition, ErrorCode, Offset}) ->
    <<Partition:32/signed-integer, ErrorCode:16/signed-integer, Offset:64/signed-integer>>.

encode_fetch_topic_response(Partition, ErrorCode, HighWaterMark, Bytes) ->
    <<Partition:32/signed-integer, ErrorCode:16/signed-integer, HighWaterMark:64/signed-integer, Bytes:32/signed-integer>>.

encode_metadata_response(Brokers, TopicMetadata) ->
    [encode_brokers(Brokers), encode_topic_metadata(TopicMetadata)].

encode_brokers(Brokers) ->
    encode_array([[<<NodeId:32/signed-integer>>, encode_string(Host), <<Port:32/signed-integer>>]
                 || {NodeId, {Host, Port}} <- Brokers]).

encode_topic_metadata(TopicMetadata) ->
    encode_array([[<<TopicErrorCode:16/signed-integer>>, encode_string(Topic), encode_partition_metadata(PartitionMetadata)]
                    || {TopicErrorCode, Topic, PartitionMetadata} <- TopicMetadata]).

encode_partition_metadata(PartitionMetadata) ->
    encode_array([[<<PartitionErrorCode:16/signed-integer, PartitionId:32/signed-integer, Leader:32/signed-integer>>,
                   encode_array([<<Replica:32/signed-integer>> || Replica <- Replicas]),
                   encode_array([<<I:32/signed-integer>> || I <- Isr])]
                  || {PartitionErrorCode, PartitionId, Leader, Replicas, Isr} <- PartitionMetadata]).

%% decode generic functions

decode_array(DecodeFun, <<Length:32/signed-integer, Rest/binary>>) ->
    decode_array(DecodeFun, Length, Rest, []);
decode_array(_, _) ->
    more.

decode_array(_, 0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
decode_array(DecodeFun, N, Rest, Acc) ->
    case DecodeFun(Rest) of
        {Element, Rest1} ->
            decode_array(DecodeFun, N-1, Rest1, [Element | Acc]);
        more -> more
    end.

decode_int32(<<I:32/signed-integer, Rest/binary>>) ->
    {I, Rest}.

decode_string(<<Size:16/signed-integer, String:Size/bytes, Rest/binary>>) ->
    {String, Rest}.

last_offset_delta(<<_LeaderEpoch:32/signed-integer, ?MAGIC_TWO:8/signed-integer,
                    _CRC:32/signed-integer, _Attributes:16/signed-integer, LastOffsetDelta:32/signed-integer, _Rest/binary>>) ->
    LastOffsetDelta;
last_offset_delta(_) ->
    0.

partial_decode_record_batch(<<>>) ->
    [];
partial_decode_record_batch(RecordBatch = <<_LeaderEpoch:32/signed-integer, ?MAGIC_TWO:8/signed-integer, CRC:32/signed-integer,
                                    _Attributes:16/signed-integer, LastOffsetDelta:32/signed-integer, _/binary>>) ->
    [#{crc => CRC,
       size => size(RecordBatch),
       last_offset_delta => LastOffsetDelta,
       record_batch => RecordBatch}].

%% unlike a produce, a replicate may send more than 1 batch
partial_decode_record_batches(<<>>) ->
    [];
partial_decode_record_batches(<<_Id:64/signed-integer, Length:32/signed-integer, RecordBatch:Length/binary, Rest/binary>>) ->
    case RecordBatch of
        <<_LeaderEpoch:32/signed-integer, ?MAGIC_TWO:8/signed-integer, CRC:32/signed-integer,
          _Attributes:16/signed-integer, LastOffsetDelta:32/signed-integer, _/binary>> ->
            [#{crc => CRC,
               size => Length,
               last_offset_delta => LastOffsetDelta,
               record_batch => RecordBatch} | partial_decode_record_batches(Rest)];
        _ ->
            []
    end.

%% decode requests

decode_metadata_request(Msg) ->
    {Topics, _rest} = decode_array(fun decode_topics/1, Msg),
    Topics.

decode_topics(<<Size:16/signed-integer, Topic:Size/bytes, Rest/binary>>) ->
    {Topic, Rest}.

decode_produce_request(<<Acks:16/signed-integer, Timeout:32/signed-integer, Topics/binary>>) ->
    {TopicsDecode, _} = decode_array(fun decode_produce_topics_request/1, Topics),
    {Acks, Timeout, TopicsDecode}.

decode_produce_topics_request(<<Size:16/signed-integer, Topic:Size/binary, Rest/binary>>) ->
    {TopicData, Rest1} = decode_array(fun decode_produce_partitions_request/1, Rest),
    {{Topic, TopicData}, Rest1}.

decode_produce_partitions_request(<<Partition:32/signed-integer, Size:32/signed-integer,
                                    RecordBatch:Size/binary, Rest/binary>>) ->
    {{Partition, partial_decode_record_batch(RecordBatch)}, Rest}.

%% decode replicate

decode_replicate_request(<<Acks:16/signed-integer, Timeout:32/signed-integer, TopicSize:16/signed-integer,
                           Topic:TopicSize/binary, Partition:32/signed-integer, ExpectedId:64/signed-integer,
                           RecordBatchSize:32/signed-integer, RecordBatch:RecordBatchSize/binary, _Rest/binary>>) ->
    {Acks, Timeout, {Topic, Partition, ExpectedId, partial_decode_record_batch(RecordBatch)}}.

%% decode responses

decode_response(<<Size:32/signed-integer, Record:Size/binary, Rest/binary>>) ->
    <<CorrelationId:32/signed-integer, Response/binary>> = Record,
    {CorrelationId, Response, Rest};
decode_response(<<Size:32/signed-integer, Rest/binary>>) ->
    {more, Size - byte_size(Rest)};
decode_response(_) ->
    more.

decode_response(?FETCH_REQUEST, Response) ->
    decode_fetch_response(Response);
decode_response(?PRODUCE_REQUEST, Response) ->
    decode_produce_response(Response);
decode_response(?REPLICATE_REQUEST, Response) ->
    decode_replicate_response(Response);
decode_response(?DELETE_TOPIC_REQUEST, Response) ->
    decode_delete_topic_response(Response);
decode_response(?TOPICS_REQUEST, Response) ->
    decode_topics_response(Response);
decode_response(?METADATA_REQUEST, Response) ->
    decode_metadata_response(Response).

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

update_chains_and_topics(Brokers, Topic, HeadId, TailId, {ChainsAcc, TopicsAcc}) ->
    {_, {HeadHost, HeadPort}} = lists:keyfind(HeadId, 1, Brokers),
    {_, {TailHost, TailPort}} = lists:keyfind(TailId, 1, Brokers),
    ChainName = <<HeadHost/binary, "-", (integer_to_binary(HeadPort))/binary>>,
    {maps:put(ChainName, #chain{name = ChainName,
                                head = {binary_to_list(HeadHost), HeadPort},
                                tail = {binary_to_list(TailHost), TailPort}}, ChainsAcc),
     maps:put(Topic, ChainName, TopicsAcc)}.


decode_metadata_response_brokers(<<NodeId:32/signed-integer, HostSize:16/signed-integer,
                                   Host:HostSize/binary, Port:32/signed-integer, Rest/binary>>) ->
    {{NodeId, {binary:copy(Host), Port}}, Rest}.

decode_metadata_response_topic_metadata(<<_ErrorCode:16/signed-integer, TopicSize:16/signed-integer,
                                          Topic:TopicSize/binary, PartitionMetadataRest/binary>>) ->
    {PartitionMetadata, Rest} = decode_array(fun decode_partition_metadata/1, PartitionMetadataRest),
    {#{topic => binary:copy(Topic),
       partitions => PartitionMetadata}, Rest}.

decode_partition_metadata(<<_PartitionErrorCode:16/signed-integer, PartitionId:32/signed-integer,
                            Leader:32/signed-integer, Rest/binary>>) ->
    {Isrs, Rest1}= decode_array(fun decode_int32/1, Rest),
    {Replicas, Rest2}= decode_array(fun decode_int32/1, Rest1),

    {#{partition_id => PartitionId,
       leader => Leader,
       isrs => Isrs,
       replicas => Replicas}, Rest2}.

decode_produce_response(Response) ->
    case decode_array(fun decode_produce_response_topics/1, Response) of
        {TopicResults, _Rest} ->
            maps:from_list(TopicResults);
        more -> more
    end.

decode_produce_response_topics(<<Size:16/signed-integer, Topic:Size/binary, PartitionsRaw/binary>>) ->
    case decode_array(fun decode_produce_response_partitions/1, PartitionsRaw) of
        {Partitions, Rest} ->
            {{Topic, maps:from_list(Partitions)}, Rest};
        more -> more
    end;
decode_produce_response_topics(_) ->
    more.

decode_produce_response_partitions(<<Partition:32/signed-integer, ErrorCode:16/signed-integer,
                                     Offset:64/signed-integer, Rest/binary>>) ->
    {{Partition, #{error_code => ErrorCode,
                   offset => Offset}}, Rest};
decode_produce_response_partitions(_) ->
    more.

decode_replicate_response(<<Partition:32/signed-integer, ?WRITE_REPAIR:16/signed-integer,
                            Size:32/signed-integer, RecordBatch:Size/binary, _Rest/binary>>) ->
    {Partition, #{error_code => ?WRITE_REPAIR, records => partial_decode_record_batches(RecordBatch)}};
decode_replicate_response(<<Partition:32/signed-integer, ErrorCode:16/signed-integer,
                            Offset:64/signed-integer, _Rest/binary>>) ->
    {Partition, #{error_code => ErrorCode, offset => Offset}};
decode_replicate_response(_) ->
    more.

decode_delete_topic_response(Data) ->
    case decode_string(Data) of
        {<<"OK">>, <<>>} -> ok;
        {Reason, <<>>} -> {error, Reason}
    end.

decode_topics_response(<<TopicsCount:32/signed-integer, Msg/binary>>) ->
    {Chains, _Rest} = decode_array(fun decode_chain/1, Msg),
    {TopicsCount, Chains}.

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

maybe_convert(start_space) ->
    <<"$$start_space">>;
maybe_convert(end_space) ->
    <<"$$end_space">>;
maybe_convert(B) when is_binary(B) ->
    B.

%% split these to unconfuse dialyzer
maybe_deconvert(<<"$$start_space">>) ->
    start_space;
maybe_deconvert(<<"$$end_space">>) ->
    end_space;
maybe_deconvert(B) when is_binary(B) ->
    B.

decode_fetch_response(Msg) ->
    case decode_array(fun decode_fetch_topic/1, Msg) of
        {Topics, _Rest} ->
            maps:from_list(Topics);
        more -> more
    end.

decode_fetch_topic(<<Sz:16/signed-integer, Topic:Sz/binary, Partitions/binary>>) ->
    case decode_array(fun decode_fetch_partition/1, Partitions) of
        {DecPartitions, Rest} ->
            {{Topic, maps:from_list(DecPartitions)}, Rest};
        more ->
            more
    end;
decode_fetch_topic(_) ->
    more.

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
-spec decode_record_batches(iodata()) -> [vg:record_batch()].
decode_record_batches(RecordBatches) ->
    decode_record_batches(RecordBatches, []).

%% TODO: validate recordsize
decode_record_batches(End, Set) when End =:= eof orelse End =:= <<>> ->
    lists:reverse(Set);
decode_record_batches(<<FirstOffset:64/signed-integer, Length:32/signed-integer, LeaderEpoch:32/signed-integer,
                    ?MAGIC_TWO:8/signed-integer, Rest/binary>>, Set) ->
    {Set1, Rest0} = decode_record_batch(FirstOffset, <<Length:32/signed-integer, LeaderEpoch:32/signed-integer,
                                                       ?MAGIC_TWO:8/signed-integer, Rest/binary>>, Set),
    decode_record_batches(Rest0, Set1);
decode_record_batches(_, Set) ->
    Set.

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
            {decode_records(FirstOffset, FirstSequenceNumber, FirstTimestamp, snappyer:decompress(Data), Set), Rest};
        ?COMPRESS_LZ4 ->
            {decode_records(FirstOffset, FirstSequenceNumber, FirstTimestamp, lz4:unpack(Data), Set), Rest}
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

decode_varint(<<1:1, Number:7, Rest/binary>>, Position, Acc) ->
    decode_varint(Rest, Position + 7, (Number bsl Position) + Acc);
decode_varint(<<0:1, Number:7, Rest/binary>>, Position, Acc) ->
    {(Number bsl Position) + Acc, Rest}.

decode_headers(Headers) ->
    decode_array(fun decode_header/1, Headers).

decode_header(Header) ->
    {KeyLength, Rest0} = decode_varint(Header, 0, 0),
    <<Key:KeyLength/binary, Rest1/binary>> = Rest0,
    {ValueLength, Rest2} = decode_varint(Rest1, 0, 0),
    <<Value:ValueLength/binary, Rest3/binary>> = Rest2,
    {{Key, Value}, Rest3}.

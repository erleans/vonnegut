-module(vg_protocol).

-export([encode_fetch/4,
         encode_produce/3,
         encode_request/4,
         encode_metadata_response/2,

         encode_array/1,
         encode_log/2,
         encode_kv/1,
         encode_string/1,
         encode_bytes/1,
         encode_produce_response/1,
         encode_fetch_response/5,
         decode_array/2,
         decode_record_set/1,

         decode_metadata_request/1,

         decode_produce_request/1,
         decode_fetch_response/1,
         decode_response/1,
         decode_response/2,
         decode_record_set/2]).

-include("vg.hrl").

%% encode requests

encode_fetch(ReplicaId, MaxWaitTime, MinBytes, Topics) ->
    [<<ReplicaId:32/signed-integer, MaxWaitTime:32/signed-integer, MinBytes:32/signed-integer>>, encode_topics(Topics)].

encode_produce(Acks, Timeout, TopicData) when Acks =:= -1
                                            ; Acks =:= 0
                                            ; Acks =:= 1 ->
    [<<Acks:16/signed-integer, Timeout:32/signed-integer>>, encode_topic_data(TopicData)].

encode_topic_data(TopicData) ->
    encode_array([[encode_string(Topic), encode_data(Data)] || {Topic, Data} <- TopicData]).

encode_data(Data) ->
    encode_array([begin
                      RecordSetEncoded = encode_record_set(RecordSet),
                      [<<Partition:32/signed-integer, (iolist_size(RecordSetEncoded)):32/signed-integer>>, RecordSetEncoded]
                  end || {Partition, RecordSet} <- Data]).

encode_topics(Topics) ->
    encode_array([[encode_string(Topic), encode_partitions(Partitions)] || {Topic, Partitions} <- Topics]).

encode_partitions(Partitions) ->
    encode_array([<<Partition:32/signed-integer, Offset:64/signed-integer, MaxBytes:32/signed-integer>>
                     || {Partition, Offset, MaxBytes} <- Partitions]).

encode_request(ApiKey, CorrelationId, ClientId, Request) ->
    [<<ApiKey:16/signed-integer, ?API_VERSION:16/signed-integer, CorrelationId:32/signed-integer>>,
     encode_string(ClientId), Request].

%% generic encode functions

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

encode_record_set([]) ->
    [];
encode_record_set(Record) when is_binary(Record) ->
    Record2 = encode_record(Record),
    [<<0:64/signed-integer, (iolist_size(Record2)):32/signed-integer>>, Record2];
encode_record_set([Record | T]) ->
    Record2 = encode_record(Record),
    [<<0:64/signed-integer, (iolist_size(Record2)):32/signed-integer>>, Record2 | encode_record_set(T)].

encode_record(Record) ->
    Record2 = [<<?MAGIC:8/signed-integer, 0:8/signed-integer>>, encode_kv(Record)],
    [<<(erlang:crc32(Record2)):32/unsigned-integer>>, Record2].

%% <<Id:64, RecordSize:32, Crc:32, MagicByte:8, Attributes:8, Key/Value>>
-spec encode_log(Id, Value) -> EncodedLog when
      Id :: integer(),
      Value :: vg:record(),
      EncodedLog :: {pos_integer(), pos_integer(), iolist()} | {error, bad_checksum}.
encode_log(Id, #{crc := CRC, record := Record}) ->
    case erlang:crc32(Record) of
        CRC ->
            RecordIoList = [<<CRC:32/unsigned-integer>>, Record],
            RecordSize = erlang:iolist_size(RecordIoList),
            {Id+1, RecordSize+12, [<<Id:64/signed-integer, RecordSize:32/signed-integer>>, RecordIoList]};
        BadChecksum ->
            lager:info("checksums don't match crc1=~p crc2=~p", [CRC, BadChecksum]),
            {error, bad_checksum}
    end;
encode_log(Id, #{record := Record}) ->
    EncodedRecord = encode_record(Record),
    RecordSize = erlang:iolist_size(EncodedRecord),
    {Id+1, RecordSize+12, [<<Id:64/signed-integer, RecordSize:32/signed-integer>>, EncodedRecord]}.

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

encode_fetch_response(Topic, Partition, ErrorCode, HighWaterMark, Bytes) ->
    encode_array([[encode_string(Topic), encode_array([<<Partition:32/signed-integer, ErrorCode:16/signed-integer,
                                                         HighWaterMark:64/signed-integer, Bytes:32/signed-integer>>])]]).

encode_metadata_response(Brokers, TopicMetadata) ->
    [encode_brokers(Brokers), encode_topic_metadata(TopicMetadata)].

encode_brokers(Brokers) ->
    encode_array([[<<NodeId:32/signed-integer>>, encode_string(Host), <<Port:32/signed-integer>>]
                 || {NodeId, Host, Port} <- Brokers]).

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
    decode_array(DecodeFun, Length, Rest, []).

decode_array(_, 0, Rest, Acc) ->
    {Acc, Rest};
decode_array(DecodeFun, N, Rest, Acc) ->
    case DecodeFun(Rest) of
        {Element, Rest1} ->
            decode_array(DecodeFun, N-1, Rest1, [Element | Acc]);
        more -> more
    end.

decode_record_set(<<>>) ->
    [];
decode_record_set(<<_:64/signed-integer, Size:32/signed-integer, Record:Size/binary, Rest/binary>>) ->
    [decode_record(Record) | decode_record_set(Rest)].

%% <<?MAGIC:8/signed-integer, Compression:8/signed-integer, -1:32/signed-integer, Size:32/signed-integer, Data:Size/binary>>
decode_record(<<CRC:32/unsigned-integer, Record/binary>>) ->
    #{crc => CRC,
      record => Record}.

%% decode requests

decode_metadata_request(Msg) ->
    {Topics, _rest} = decode_array(fun decode_topics/1, Msg),
    Topics.

decode_produce_request(<<Acks:16/signed-integer, Timeout:32/signed-integer, Topics/binary>>) ->
    {TopicsDecode, _} = decode_array(fun decode_produce_topics_request/1, Topics),
    {Acks, Timeout, TopicsDecode}.

decode_produce_topics_request(<<Size:16/signed-integer, Topic:Size/binary, Rest/binary>>) ->
    {TopicData, Rest1} = decode_array(fun decode_produce_partitions_request/1, Rest),
    {{Topic, TopicData}, Rest1}.

decode_produce_partitions_request(<<Partition:32/signed-integer, Size:32/signed-integer,
                                    RecordSet:Size/binary, Rest/binary>>) ->
    {{Partition, decode_record_set(RecordSet)}, Rest}.

%% decode responses

decode_response(<<Size:32/signed-integer, Record:Size/binary, Rest/binary>>) ->
    <<CorrelationId:32/signed-integer, Response/binary>> = Record,
    {CorrelationId, Response, Rest};
decode_response(_) ->
    more.

decode_response(?FETCH_REQUEST, Response) ->
    decode_fetch_response(Response);
decode_response(?PRODUCE_REQUEST, Response) ->
    decode_produce_response(Response);
decode_response(?TOPICS_REQUEST, Response) ->
    decode_topics_response(Response).

decode_produce_response(Response) ->
    {TopicResults, _Rest}= decode_array(fun decode_produce_response_topics/1, Response),
    [{Topic, [{Partition, ErrorCode, Offset}]}] = TopicResults,
    %% we're currently not including throttle time or timestamp
    #{topic => Topic,
      partition => Partition,
      offset => Offset,
      error_code => ErrorCode}.

decode_produce_response_topics(<<Size:16/signed-integer, Topic:Size/binary, PartitionsRaw/binary>>) ->
    {Partitions, Rest} = decode_array(fun decode_produce_response_partitions/1, PartitionsRaw),
    {{Topic, Partitions}, Rest}.

decode_produce_response_partitions(<<Partition:32/signed-integer, ErrorCode:16/signed-integer,
                                     Offset:64/signed-integer, Rest/binary>>) ->
    {{Partition, ErrorCode, Offset}, Rest}.

decode_topics_response(Msg) ->
    {Topics, _Rest} = decode_array(fun decode_topics/1, Msg),
    Topics.

decode_topics(<<Size:16/signed-integer, Topic:Size/bytes, Rest/binary>>) ->
    {Topic, Rest}.

decode_fetch_response(Msg) ->
    case decode_array(fun decode_fetch_topic/1, Msg) of
        %% TODO: unwrapping the topic here because we don't currently
        %% handle multiple topics.  it might be easiest to roll this
        %% up into a map of topics => responses rather than an array
        {[Topic], _Rest} ->
            Topic;
        more -> more
    end.

decode_fetch_topic(<<Sz:16/signed-integer, Topic:Sz/binary, Partitions/binary>>) ->
    case decode_array(fun decode_fetch_partition/1, Partitions) of
        {DecPartitions, Rest} ->
            {#{topic => Topic, partitions => DecPartitions}, Rest};
        more -> more
    end.

decode_fetch_partition(<<Partition:32/signed-integer, ErrorCode:16/signed-integer,
                         HighWaterMark:64/signed-integer, Bytes:32/signed-integer, RecordSet:Bytes/bytes,
                         Rest/binary>>) ->
    %% there are a number of fields that we're not including currently:
    %% - throttle time
    %% - topic name
    {#{high_water_mark => HighWaterMark,
       partition => Partition,
       record_set_size => Bytes,
       error_code => ErrorCode,
       record_set => decode_record_set(RecordSet, [])}, Rest};
decode_fetch_partition(_) ->
    more.

%% TODO: validate recordsize
decode_record_set(End, Set) when End =:= eof orelse End =:= <<>> ->
    lists:reverse(Set);
decode_record_set(<<Id:64/signed-integer, _RecordSize:32/signed-integer, Crc:32/signed-integer,
                    ?MAGIC:8/signed-integer, Attributes:8/signed-integer, -1:32/signed-integer,
                    ValueSize:32/signed-integer, Value:ValueSize/binary, Rest/binary>>, Set) ->
    case ?COMPRESSION(Attributes) of
        ?COMPRESS_NONE ->
            decode_record_set(Rest, [#{id => Id,
                                       crc => Crc,
                                       record => Value} | Set]);
        ?COMPRESS_GZIP ->
            decode_record_set(Rest, decode_record_set(zlib:gunzip(Value), Set));
        ?COMPRESS_SNAPPY ->
            decode_record_set(Rest, decode_record_set(snappyer:decompress(Value), Set));
        ?COMPRESS_LZ4 ->
            decode_record_set(Rest, decode_record_set(lz4:unpack(Value), Set))
    end;
%% compression only allowed if key is null
decode_record_set(<<Id:64/signed-integer, _RecordSize:32/signed-integer, Crc:32/signed-integer,
                    ?MAGIC:8/signed-integer, _Attributes:8/signed-integer,
                    KeySize:32/signed-integer, Key:KeySize/binary, ValueSize:32/signed-integer,
                    Value:ValueSize/binary, Rest/binary>>, Set) ->
    ct:pal("KEY ~p ~p", [Key, Value]),
    decode_record_set(Rest, [#{id => Id,
                               crc => Crc,
                               record => {Key, Value}} | Set]).

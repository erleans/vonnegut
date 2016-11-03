-module(vg_protocol).

-export([encode_fetch/4,
         encode_produce/3,
         encode_request/4,
         encode_message/2,
         encode_string/1,
         encode_bytes/1,

         decode_array/2,

         decode_produce_request/1,
         decode_fetch_response/1,
         decode_response/1,
         decode_response/2]).

-include("vg.hrl").

encode_fetch(ReplicaId, MaxWaitTime, MinBytes, Topics) ->
    [<<ReplicaId:32/signed, MaxWaitTime:32/signed, MinBytes:32/signed>>, encode_topics(Topics)].

encode_produce(Acks, Timeout, TopicData) when Acks =:= -1
                                            ; Acks =:= 0
                                            ; Acks =:= 1 ->
    [<<Acks:16/signed, Timeout:32/signed>>, encode_topic_data(TopicData)].

encode_topic_data(TopicData) ->
    encode_array([[encode_string(Topic), encode_data(Data)] || {Topic, Data} <- TopicData]).

encode_data(Data) ->
    encode_array([[<<Partition:32/signed>>, encode_bytes(RecordSet)] || {Partition, RecordSet} <- Data]).

encode_topics(Topics) ->
    encode_array([[encode_string(Topic), encode_partitions(Partitions)] || {Topic, Partitions} <- Topics]).

encode_partitions(Partitions) ->
    encode_array([<<Partition:32/signed, Offset:64/signed, MaxBytes:32/signed>>
                     || {Partition, Offset, MaxBytes} <- Partitions]).

encode_request(ApiKey, CorrelationId, ClientId, Request) ->
    [<<ApiKey:16, ?API_VERSION:16, CorrelationId:32>>, encode_string(ClientId), Request].

encode_string(undefined) ->
    <<-1:32/signed>>;
encode_string(Data) when is_binary(Data) ->
    [<<(size(Data)):32>>, Data];
encode_string(Data) ->
    [<<(length(Data)):32>>, Data].

encode_array(Array) ->
    [<<(length(Array)):32>>, Array].

encode_bytes(undefined) ->
    <<-1:32/signed>>;
encode_bytes(Data) ->
    [<<(size(Data)):32>>, Data].

%% <<Id:64, MessageSize:32, Crc:32, MagicByte:8, Attributes:8, Key/Value>>
-spec encode_message(Id, Values) -> EncodedLog when
      Id :: integer(),
      Values :: binary() | {binary(), binary()},
      EncodedLog :: {pos_integer(), pos_integer(), iolist()}.
encode_message(Id, KeyValue) ->
    KV = encode_kv(KeyValue),
    CRC = erlang:crc32(KV),
    MessageIoList = [<<CRC:32/signed, ?MAGIC:8/signed, ?ATTRIBUTES:8/signed>>, KV],
    MessageSize = erlang:iolist_size(MessageIoList),
    {Id+1, MessageSize+12, [<<Id:64/signed, MessageSize:32/signed>>, MessageIoList]}.

encode_kv({Key, Value}) ->
    [<<(erlang:byte_size(Key)):32/signed>>, Key, <<(erlang:byte_size(Value)):32/signed>>, Value];
encode_kv(Value) ->
    <<-1:32/signed, (erlang:byte_size(Value)):32/signed, Value/binary>>.

%% decode generic functions

decode_array(DecodeFun, <<Length:32/signed, Rest/binary>>) ->
    decode_array(DecodeFun, Length, Rest, []).

decode_array(_, 0, Rest, Acc) ->
    {Acc, Rest};
decode_array(DecodeFun, N, Rest, Acc) ->
    {Element, Rest1} = DecodeFun(Rest),
    decode_array(DecodeFun, N-1, Rest1, [Element | Acc]).

%% decode requests

decode_produce_request(<<Acks:16/signed, Timeout:32/signed, Topics/binary>>) ->
    {TopicsDecode, _} = decode_array(fun decode_produce_topics_request/1, Topics),
    {Acks, Timeout, TopicsDecode}.

decode_produce_topics_request(<<Size:32/signed, Topic:Size/binary, Rest/binary>>) ->
    {TopicData, Rest1} = decode_array(fun decode_produce_partitions_request/1, Rest),
    {{Topic, TopicData}, Rest1}.

decode_produce_partitions_request(<<Partition:32/signed, Size:32/signed, RecordSet:Size/binary, Rest/binary>>) ->
    {{Partition, RecordSet}, Rest}.

%% decode responses

decode_response(<<Size:32/signed, Message:Size/binary, Rest/binary>>) ->
    <<CorrelationId:32/signed, Response/binary>> = Message,
    {CorrelationId, Response, Rest};
decode_response(_) ->
    more.

decode_response(?FETCH_REQUEST, Response) ->
    decode_fetch_response(Response);
decode_response(?PRODUCE_REQUEST, Response) ->
    decode_produce_response(Response).

decode_produce_response(<<1:32/signed>>) ->
    ok.

decode_fetch_response(eof) ->
    [];
decode_fetch_response(Messages) ->
    decode_fetch_response(Messages, []).

decode_fetch_response(<<>>, Acc) ->
    Acc;
decode_fetch_response(<<_Id:64/signed, _MessageSize:32/signed, _CRC:32/signed, ?MAGIC:8/signed, ?ATTRIBUTES:8/signed,
                -1:32/signed, ValueSize:32/signed, KV:ValueSize/binary, Rest1/binary>>, Acc) ->
    decode_fetch_response(Rest1, [KV | Acc]);
decode_fetch_response(_Data, []) ->
    more.

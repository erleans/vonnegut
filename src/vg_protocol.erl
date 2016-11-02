-module(vg_protocol).

-export([encode_fetch/4,
         encode_request/4,
         encode_message/2,
         encode_string/1,
         encode_bytes/1,
         decode_fetch/1,
         decode_topics/1]).

-include("vg.hrl").

encode_fetch(ReplicaId, MaxWaitTime, MinBytes, Topics) ->
    [<<ReplicaId:32/signed, MaxWaitTime:32/signed, MinBytes:32/signed>>, encode_topics(Topics)].

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

decode_fetch(<<Size:32/signed, Message:Size/binary, Rest/binary>>) ->
    <<CorrelationId:32/signed, Topics/binary>> = Message,
    {{CorrelationId, decode_topics(Topics)}, Rest};
decode_fetch(_) ->
    more.

decode_topics(eof) ->
    [];
decode_topics(Messages) ->
    decode_topics(Messages, []).

decode_topics(<<>>, Acc) ->
    Acc;
decode_topics(<<_Id:64/signed, _MessageSize:32/signed, _CRC:32/signed, ?MAGIC:8/signed, ?ATTRIBUTES:8/signed,
         -1:32/signed, ValueSize:32/signed, KV:ValueSize/binary, Rest1/binary>>, Acc) ->
    decode_topics(Rest1, [KV | Acc]);
decode_topics(_Data, []) ->
    more.

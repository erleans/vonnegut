-module(vg_encode).

-export([message/2]).

-define(MAGIC, 0).
-define(ATTRIBUTES, 0).

%% <<Id:64, MessageSize:32, Crc:32, MagicByte:8, Attributes:8, Key/Value>>
-spec message(Id, Values) -> EncodedLog when
      Id :: integer(),
      Values :: binary() | {bitstring(), bitstring()},
      EncodedLog :: {integer(), pos_integer(), iolist()}.
message(Id, KeyValue) ->
    KV = encode_kv(KeyValue),
    CRC = erlang:crc32(KV),
    MessageIoList = [<<CRC:32/signed, ?MAGIC:8/signed, ?ATTRIBUTES:8/signed>>, KV],
    MessageSize = erlang:iolist_size(MessageIoList),
    {Id+1, MessageSize+12, [<<Id:64/signed, MessageSize:32/signed>>, MessageIoList]}.

encode_kv({Key, Value}) ->
    [<<(erlang:byte_size(Key)):32/signed>>, Key, <<(erlang:byte_size(Value)):32/signed>>, Value];
encode_kv(Value) ->
    <<-1:32/signed, (erlang:byte_size(Value)):32/signed, Value/binary>>.

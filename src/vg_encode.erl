-module(vg_encode).

-export([message/2,
         decode/1]).

-define(MAGIC, 0).
-define(ATTRIBUTES, 0).

%% <<Id:64, MessageSize:32, Crc:32, MagicByte:8, Attributes:8, Key/Value>>
-spec message(Id, Values) -> EncodedLog when
      Id :: integer(),
      Values :: binary() | {binary(), binary()},
      EncodedLog :: iolist().
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

decode(eof) ->
    [];
decode(Messages) ->
    decode(Messages, []).

decode(<<>>, Acc) ->
    Acc;
decode(<<_Id:64/signed, MessageSize:32/signed, _CRC:32/signed, ?MAGIC:8/signed, ?ATTRIBUTES:8/signed,
         -1:32/signed, ValueSize:32/signed, KV:ValueSize/binary, Rest1/binary>>, Acc) ->
    decode(Rest1, [KV | Acc]).

-module(vg_encode).

-export([message_set/2]).

-define(MAGIC, 0).
-define(ATTRIBUTES, 0).

-spec message_set(Id, Values) -> EncodedLog when
      Id :: integer(),
      Values :: binary(),
      EncodedLog :: iolist().
message_set(Id, Values) ->
    message_set(Id, Values, []).

%% <<Id:64, MessageSize:32, Crc:32, MagicByte:8, Attributes:8, Key/Value>>
message_set(Id, [], Acc) ->
    {Id, Acc};
message_set(Id, [Value | T], Acc) ->
    CRC = erlang:crc32(Value),
    MessageIoList = [<<CRC:32/signed, ?MAGIC:8/signed, ?ATTRIBUTES:8/signed>>, Value],
    MessageSize = erlang:iolist_size(MessageIoList),
    message_set(Id+1, T, [Acc | [<<Id:64/signed, MessageSize:32/signed>>, MessageIoList]]).

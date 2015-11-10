-module(vg_encode).

-export([message_set/2]).

-spec message_set(Id, MessageSet) -> EncodedLog when
      Id :: integer(),
      MessageSet :: binary(),
      EncodedLog :: iolist().
message_set(Id, MessageSet) ->
    %% Size = erlang:size(Log),
    %% Log1 = [<<Size:32/signed>>, Log],
    message_set(Id, MessageSet, []).

message_set(Id, [], Acc) ->
    {Id, Acc};
message_set(Id, [Message | T], Acc) ->
    CRC = erlang:crc32(Message),
    LogIolist = [<<CRC:32/signed>>, Message],
    LogSize = erlang:iolist_size(LogIolist),
    message_set(Id+1, T, [Acc | [<<Id:64/signed, LogSize:32/signed>>, LogIolist]]).

-module(vg_encode).

-export([log/2]).

-spec log(Id, Log) -> EncodedLog when
      Id :: binary(),
      Log :: binary(),
      EncodedLog :: iolist().
log(Id, Log) ->
    %% Size = erlang:size(Log),
    %% Log1 = [<<Size:32/signed>>, Log],
    CRC = erlang:crc32(Log),
    LogIolist = [<<CRC:32/signed>>, Log],
    LogSize = erlang:iolist_size(LogIolist),
    [<<Id:64/signed, LogSize:32/signed>>, LogIolist].

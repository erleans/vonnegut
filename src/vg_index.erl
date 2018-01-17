%% Index files are named [offset].index
%% Entries in the index are <<(Id-Offset):32/unsigned, Position:32/unsigned>>
%% Position is the offset in [offset].log to find the log Id
-module(vg_index).

-include("vg.hrl").

-export([find_in_index/3]).

-spec find_in_index(Fd, BaseOffset, Id) -> integer() | not_found when
      Fd         :: file:fd(),
      BaseOffset :: integer(),
      Id         :: integer().
find_in_index(Fd, BaseOffset, Id) ->
    case file:read(Fd, (2 * ?INDEX_ENTRY_SIZE)) of
        {ok, Bytes} ->
            find_in_index_(Fd, Id, BaseOffset, Bytes);
        _ ->
            0
    end.

%% Optimize later. Could keep entire index in memory
%% and could (in memory or not) use a binary search
find_in_index_(_, _, _, <<>>) ->
    0;
%% special case for when below the first offset in a single entry index
find_in_index_(_, Id, BaseOffset, <<Offset:?INDEX_OFFSET_BITS/unsigned,
                                    _Pos:?INDEX_POS_BITS/unsigned>>)
  when BaseOffset + Offset > Id->
    0;
find_in_index_(_, _, _, <<_Offset:?INDEX_OFFSET_BITS/unsigned,
                          Position:?INDEX_POS_BITS/unsigned>>) ->
    Position;
find_in_index_(_, Id, BaseOffset, <<Offset:?INDEX_OFFSET_BITS/unsigned,
                                    Position:?INDEX_POS_BITS/unsigned, _/binary>>)
  when Id =:= BaseOffset + Offset ->
    Position;
%% special case for below the first offset in a multi-entry index, but
%% I worry that it might be overly broad.
find_in_index_(_, Id, BaseOffset, <<Offset:?INDEX_OFFSET_BITS/unsigned,
                                    _Pos:?INDEX_POS_BITS/unsigned,
                                    _Offset2:?INDEX_OFFSET_BITS/unsigned,
                                    _Pos2:?INDEX_POS_BITS/unsigned, _/binary>>)
  when BaseOffset + Offset > Id ->
    0;
find_in_index_(_, Id, BaseOffset, <<_Offset:?INDEX_OFFSET_BITS/unsigned,
                                    Position:?INDEX_POS_BITS/unsigned,
                                    Offset2:?INDEX_OFFSET_BITS/unsigned,
                                    _Pos2:?INDEX_POS_BITS/unsigned, _/binary>>)
  when BaseOffset + Offset2 > Id ->
    Position;
find_in_index_(Fd, Id, BaseOffset, <<_Offset:?INDEX_OFFSET_BITS/unsigned,
                                     _Pos:?INDEX_POS_BITS/unsigned, Rest/binary>>) ->
    case file:read(Fd, ?INDEX_ENTRY_SIZE) of
        {ok, Bytes} ->
            find_in_index_(Fd, Id, BaseOffset, <<Rest/binary, Bytes/binary>>);
        _ ->
            find_in_index_(Fd, Id, BaseOffset, Rest)
    end.

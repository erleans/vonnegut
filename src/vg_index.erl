%% Index files are named [offset].index
%% Entries in the index are <<(Id-Offset):24/signed, Position:24/signed>>
%% Position is the offset in [offset].log to find the log Id
-module(vg_index).

-export([find_in_index/2,
         read_index/1]).

%% Parse index into searchable format
read_index(TopicDirBinary) ->
    TopicDir = binary_to_list(TopicDirBinary),
    IndexFiles = lists:sort(filelib:wildcard(filename:join(TopicDir, "*.index"))),
    lists:map(fun(File) ->
                      Offset = list_to_integer(filename:basename(File, ".index")),
                      {ok, Binary} = file:read_file(File),
                      {Offset, lists:reverse(index_to_list(Binary))}
              end, IndexFiles).

index_to_list(Binary) ->
    index_to_list(Binary, []).

index_to_list(<<>>, Acc) ->
    Acc;
index_to_list(<<Id:24/signed, Position:24/signed>>, Acc) ->
    [{Id, Position} | Acc];
index_to_list(<<Id:24/signed, Position:24/signed, Rest/binary>>, Acc) ->
    index_to_list(Rest, [{Id, Position} | Acc]).

%% Find the logical position in the log file from the index
find_in_index(_Id, []) ->
    {0, 0};
find_in_index(Id, [{Offset, Segments}]) ->
    {Offset, find_pos_in_index(Id, Offset, Segments)};
find_in_index(Id, [{_, _}, {Offset, Segments} | _]) when Offset =:= Id ->
    {Offset, find_pos_in_index(Id, Offset, Segments)};
find_in_index(Id, [{Offset, Segments}, {NextOffset, _} | _]) when NextOffset > Id ->
    {Offset, find_pos_in_index(Id, Offset, Segments)};
find_in_index(Id, [{_, _} | Rest]) ->
    find_in_index(Id, Rest).

find_pos_in_index(_Id, _, []) ->
    0;
find_pos_in_index(Id, Offset, [{X, _}]) when Offset + X > Id ->
    0;
find_pos_in_index(_Id, _, [{_, Position}]) ->
    Position;
find_pos_in_index(Id, Offset, [{_, _}, {Y, Position} | _]) when Offset + Y =:= Id ->
    Position;
find_pos_in_index(Id, Offset, [{_, Position}, {Y, _} | _]) when Offset + Y > Id ->
    Position;
find_pos_in_index(Id, Offset, [{_, _}, {_, _} | Rest]) ->
    find_pos_in_index(Id, Offset, Rest).

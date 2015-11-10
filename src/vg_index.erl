%% Index files are named [offset].index
%% Entries in the index are <<(Id-Offset):24/signed, Position:24/signed>>
%% Position is the offset in [offset].log to find the log Id
-module(vg_index).

-export([find_in_index/2]).

-spec find_in_index(Topic, Id) -> integer() | not_found when
      Topic :: binary(),
      Id    :: integer().
find_in_index(Topic, Id) ->
    find_in_index(Topic, <<"0">>, Id).

-spec find_in_index(Topic, Partition, Id) -> integer() | not_found when
      Topic     :: binary(),
      Partition :: binary(),
      Id        :: integer().
find_in_index(Topic, Partition, Id) ->
    {ok, [LogDir]} = application:get_env(vonnegut, log_dirs),
    TopicDir = filename:join(ec_cnv:to_list(LogDir), [ec_cnv:to_list(Topic), "-", ec_cnv:to_list(Partition)]),
    IndexFiles = lists:sort(filelib:wildcard(filename:join(TopicDir, "*.index"))),
    case find_in_index_(Id, IndexFiles) of
        {Offset, Position} ->
            {vg_utils:log_file(TopicDir, Offset), Position};
        not_found ->
            not_found
    end.

find_in_index_(Id, Files) ->
    case find_index_file(Id, Files) of
        {Offset, File} ->
            {ok, Binary} = file:read_file(File),
            {Offset, find_in_index_file(Id, Offset, Binary)};
        _ ->
            not_found
    end.

find_index_file(_, []) ->
    not_found;
find_index_file(Id, [X]) ->
    case list_to_integer(filename:basename(X, ".index")) of
        XOffset when XOffset =< Id ->
            {XOffset, X};
        _ ->
            not_found
    end;
find_index_file(Id, [X, Y | T]) ->
    XOffset = list_to_integer(filename:basename(X, ".index")),
    case list_to_integer(filename:basename(Y, ".index")) of
        YOffset when YOffset > Id ->
            {XOffset, X};
        YOffset when YOffset =:= Id ->
            {YOffset, Y};
        _ ->
            find_index_file(Id, T)
    end.

find_in_index_file(_, _, <<>>) ->
    0;
find_in_index_file(_, _, <<_:24/signed, Position:24/signed>>) ->
    Position;
find_in_index_file(Id, BaseOffset, <<Offset:24/signed, Position:24/signed, _/binary>>)
  when Id =:= BaseOffset + Offset ->
    Position;
find_in_index_file(Id, BaseOffset, <<_:24/signed, Position:24/signed, Offset:24/signed, _:24/signed, _/binary>>)
  when BaseOffset + Offset > Id ->
    Position;
find_in_index_file(Id, BaseOffset, <<_:24/signed, _:24/signed, Rest/binary>>) ->
    find_in_index_file(Id, BaseOffset, Rest).

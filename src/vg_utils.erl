-module(vg_utils).

-compile([warn_missing_spec_all, warn_untyped_record]).

-export([index_file/2,
         index_file/3,
         log_file/2,
         log_file/3,
         topic_dir/2,
         open_append/1,
         open_read/1,

         topics_on_disk/0,

         to_atom/1,
         to_integer/1]).

%% Convenience functions for creating index and log file names
-spec index_file(file:filename_all(), integer()) -> file:filename_all().
index_file(TopicDir, Id) ->
    filename:join(TopicDir, io_lib:format("~20.10.0b.index", [Id])).

-spec index_file(vg:topic(), vg:partition(), integer()) -> file:filename_all().
index_file(Topic, Partition, Id) ->
    TopicDir = topic_dir(Topic, Partition),
    filename:join(TopicDir, io_lib:format("~20.10.0b.index", [Id])).

-spec log_file(vg:topic(), vg:partition(), integer()) -> file:filename_all().
log_file(Topic, Partition, Id) ->
    TopicDir = topic_dir(Topic, Partition),
    filename:join(TopicDir, io_lib:format("~20.10.0b.log", [Id])).

-spec log_file(file:filename_all(), integer()) -> file:filename_all().
log_file(TopicDir, Id) ->
    filename:join(TopicDir, io_lib:format("~20.10.0b.log", [Id])).

-spec topic_dir(vg:topic(), vg:partition()) -> file:filename_all().
topic_dir(Topic, Partition) ->
    {ok, [LogDir | _]} = application:get_env(vonnegut, log_dirs),
    filename:join(LogDir, [binary_to_list(Topic), "-", integer_to_list(Partition)]).

-spec topics_on_disk() -> [{vg:topic(), [vg:partition()]}].
topics_on_disk() ->
    {ok, [DataDir| _]} = application:get_env(vonnegut, log_dirs),
    TopicPartitions = filelib:wildcard(filename:join(DataDir, "*")),
    TPDict = lists:foldl(fun(TP, Acc) ->
                                 case string:tokens(filename:basename(TP), "-") of
                                     [_] ->
                                         Acc;
                                     L ->
                                         [P | TopicR] = lists:reverse(L),
                                         T = string:join(lists:reverse(TopicR), "-"),
                                         dict:append_list(list_to_binary(T), [list_to_integer(P)], Acc)
                                 end
                         end, dict:new(), TopicPartitions),
    dict:to_list(TPDict).


-spec open_append(file:filename_all()) -> {ok, file:io_device()} | {error, file:posix() | badarg | system_limit}.
open_append(Filename) ->
    case application:get_env(vonnegut, delayed_write) of
        {ok, true} ->
            %% Buffer writes up to DelayedWriteSize bytes or DelayMS milliseconds to save on OS calls
            {ok, DelayedWriteSize} = application:get_env(vonnegut, delayed_write_byte_size),
            {ok, DelayMS} = application:get_env(vonnegut, delayed_write_milliseconds),
            file:open(Filename, [append, raw, binary, {delayed_write, DelayedWriteSize, DelayMS}]);
        _ ->
            file:open(Filename, [append, raw, binary])
    end.

-spec open_read(file:filename_all()) -> {ok, file:io_device()} | {error, file:posix() | badarg | system_limit}.
open_read(Filename) ->
    file:open(Filename, [read, raw, binary]).

-spec to_integer(integer() | string() | binary()) -> integer().
to_integer(I) when is_integer(I) -> I;
to_integer(I) when is_list(I)    -> list_to_integer(I);
to_integer(I) when is_binary(I)  -> binary_to_integer(I);
to_integer(_)                    -> throw(badarg).

-spec to_atom(atom() | string() | binary()) -> atom().
to_atom(A) when is_list(A)   -> list_to_atom(A);
to_atom(A) when is_binary(A) -> binary_to_atom(A, utf8);
to_atom(A) when is_atom(A)   -> A;
to_atom(_)                   -> throw(badarg).

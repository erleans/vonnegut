-module(vg_pending_writes).

-export([ensure_tab/2,
         ack/3,
         add/3]).

-define(TAB_OPTIONS, [public, named_table, bag, {write_concurrency, true}]).
-define(TAB(Topic, Partition), binary_to_atom(<<Topic/binary, Partition>>, utf8)).

-spec ensure_tab(Topic :: binary(), Partition :: integer()) -> atom().
ensure_tab(Topic, Partition) ->
    Tab = ?TAB(Topic, Partition),
    case ets:info(Tab, name) of
        undefined ->
            ets:new(Tab, ?TAB_OPTIONS);
        _ ->
            ok
    end,
    Tab.

-spec ack(Topic :: binary(), Partition :: integer(), LatestId :: integer()) -> integer().
ack(Topic, Partition, LatestId) ->
    ets:select_delete(?TAB(Topic, Partition), [{{'$1', '$2'},
                                                [{is_integer, '$1'}, {'=<', '$1', LatestId}],
                                                [true]}]).

-spec add(Tab :: atom(), Id :: integer(), Msg :: iolist()) -> true.
add(Tab, Id, Msg) ->
    ets:insert(Tab, {Id, Msg}).

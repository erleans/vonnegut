-module(vg_pending_writes).

-export([ensure_tab/2,
         ack/2,
         add/4]).

-define(TAB_OPTIONS, [public, named_table, bag, {write_concurrency, true}]).
-define(TAB(Topic, Partition), binary_to_atom(<<Topic/binary,
                                                (integer_to_binary(Partition))/binary>>,
                                              utf8)).

-spec ensure_tab(Topic :: binary(), Partition :: integer()) -> atom().
ensure_tab(Topic, Partition) ->
    Tab = ?TAB(Topic, Partition),
    case ets:info(Tab, name) of
        undefined ->
            lager:debug("starting new acks table: ~p", [Tab]),
            ets:new(Tab, ?TAB_OPTIONS);
        _ ->
            ok
    end,
    Tab.

-spec ack(Tab :: atom(), LatestId :: integer()) -> [{integer(), _, _}].
ack(Tab, LatestId) ->
    Acked = ets:select(Tab, [{{'$1', '$2', '$3'},
                              [{is_integer, '$1'}, {'=<', '$1', LatestId}],
                              ['$_']}]),
    %% faster to just iteratively delete/2 here?  probably not a perf issue
    ets:select_delete(Tab, [{{'$1', '$2', '$3'},
                             [{is_integer, '$1'}, {'=<', '$1', LatestId}],
                             [true]}]),
    Acked.

-spec add(Tab :: atom(), Id :: integer(), From :: term(), Msg :: iolist()) -> true.
add(Tab, Id, From, Msg) ->
    ets:insert(Tab, {Id, From, Msg}).

-module(vg_topics).

-compile([warn_missing_spec_all, warn_untyped_record]).

-export([init_table/0,

         all/0,
         get_chain/1,

         insert_hwm/3,
         lookup_hwm/2,
         update_hwm/3,
         delete_hwm/2]).

-include("vg.hrl").

-define(HWM_POS, 2). %% {{Topic, Partition}, HighWaterMark}

-spec init_table() -> ets:table().
init_table() ->
    ets:new(?WATERMARK_TABLE, [set, public, named_table, {write_concurrency, true}]).

-spec all() -> [vg:topic()].
all() ->
    %% replace with ets table keys
    {Topics, _Chains, _Epoch} = vg_cluster_mgr:get_map(),
    maps:keys(Topics).

-spec get_chain(vg:topic()) -> chain() | not_found.
get_chain(Topic) ->
    %% replace with ets table lookup
    {Topics, Chains, _Epoch} = vg_cluster_mgr:get_map(),
    case maps:get(Topic, Topics, not_found) of
        not_found ->
            logger:info("lookup for non-existant topic ~p", [Topic]),
            not_found;
        Chain ->
            maps:get(Chain, Chains)
    end.

-spec insert_hwm(vg:topic(), vg:partition(), integer()) -> true.
insert_hwm(Topic, Partition, HWM) ->
    ets:insert(?WATERMARK_TABLE, {{Topic, Partition}, HWM}).

-spec lookup_hwm(vg:topic(), vg:partition()) -> integer().
lookup_hwm(Topic, Partition) ->
    try ets:lookup_element(?WATERMARK_TABLE, {Topic, Partition}, ?HWM_POS)
    catch
        error:badarg ->
            %% maybe just not loaded, try to get from disk first
            TopicDir = vg_utils:topic_dir(Topic, Partition),
            try vg_log_segments:find_latest_id(TopicDir, Topic, Partition) of
                {HWM, _, _} ->
                    ets:insert(?WATERMARK_TABLE, {{Topic, Partition}, HWM}),
                    HWM
            catch error:{badmatch,{error,enoent}} ->
                    throw({topic_not_found, Topic, Partition})
            end
    end.

-spec update_hwm(vg:topic(), vg:partition(), integer()) -> true.
update_hwm(Topic, Partition, HWMUpdate) ->
    try
        true = ets:update_element(?WATERMARK_TABLE, {Topic, Partition}, {?HWM_POS, HWMUpdate})
    catch
        error:badarg ->
            throw(hwm_table_not_loaded)
    end.

-spec delete_hwm(vg:topic(), vg:partition()) -> true.
delete_hwm(Topic, Partition) ->
    ets:delete(?WATERMARK_TABLE, {Topic, Partition}).

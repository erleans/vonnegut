-module(vg_topics).

-export([init_table/0,

         all/0,
         get_chain/1,

         insert_hwm/3,
         lookup_hwm/2,
         update_hwm/3]).

-include("vg.hrl").

-define(HWM_POS, 2). %% {{Topic, Partition}, HighWaterMark}

init_table() ->
    ets:new(?WATERMARK_TABLE, [set, public, named_table, {write_concurrency, true}]).

all() ->
    %% replace with ets table keys
    {Topics, _Chains, _Epoch} = vg_cluster_mgr:get_map(),
    maps:keys(Topics).

get_chain(Topic) ->
    %% replace with ets table lookup
    {Topics, Chains, _Epoch} = vg_cluster_mgr:get_map(),
    case maps:get(Topic, Topics, not_found) of
        not_found ->
            lager:info("lookup for non-existant topic ~p", [Topic]),
            not_found;
        Chain ->
            maps:get(Chain, Chains)
    end.

insert_hwm(Topic, Partition, HWM) ->
    ets:insert(?WATERMARK_TABLE, {{Topic, Partition}, HWM}).

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

update_hwm(Topic, Partition, HWMUpdate) ->
    try
        true = ets:update_element(?WATERMARK_TABLE, {Topic, Partition}, {?HWM_POS, HWMUpdate})
    catch
        error:badarg ->
            throw(hwm_table_not_loaded)
    end.

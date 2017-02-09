-module(vg_topics).

-export([init_table/0,

         get_chain/1,

         insert_hwm/3,
         lookup_hwm/2,
         update_hwm/3]).

-include("vg.hrl").

-define(HWM_POS, 2). %% {{Topic, Partition}, HighWaterMark}

init_table() ->
    ets:new(?WATERMARK_TABLE, [set, public, named_table, {write_concurrency, true}]).

get_chain(Topic) ->
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
    ets:lookup_element(?WATERMARK_TABLE, {Topic, Partition}, ?HWM_POS).

update_hwm(Topic, Partition, HWMUpdate) ->
    ets:update_element(?WATERMARK_TABLE, {Topic, Partition}, {?HWM_POS, HWMUpdate}).

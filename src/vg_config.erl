-module(vg_config).

-export([chain_name/0,
         port/0,
         cluster_type/0,
         replicas/0]).

-define(DEFAULT_PORT, 5588).

-type cluster_type() :: local | {direct, [any()]} | {srv, string()} | none.

-export_types([cluster_type/0]).

-spec chain_name() -> vg_chain_state:chain_name().
chain_name() ->
    vg_utils:to_atom(from_chain(name, solo)).

-spec port() -> integer().
port() ->
    vg_utils:to_integer(from_chain(port, ?DEFAULT_PORT)).

-spec cluster_type() -> cluster_type().
cluster_type() ->
    case from_chain(discovery, local) of
        "local" ->
            local;
        local ->
            local;
        {direct, Nodes} ->
            {direct, Nodes};
        {srv, Domain} ->
            {srv, Domain};
        Other ->
            lager:error("Unknown clustering option: ~p", [Other]),
            none
    end.

-spec replicas() -> integer().
replicas() ->
    case cluster_type() of
        {direct, List} ->
            length(List);
        {srv, _} ->
            vg_utils:to_integer(from_chain(replicas, 1));
        _ ->
            1
    end.

%% internal functions

from_chain(Key, Default) ->
    case application:get_env(vonnegut, chain, []) of
        [] ->
            Default;
        Chain ->
            proplists:get_value(Key, Chain, Default)
    end.

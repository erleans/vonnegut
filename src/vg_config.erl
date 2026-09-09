-module(vg_config).

-compile([warn_missing_spec_all, warn_untyped_record]).

-export([chain_name/0,
         port/0,
         cluster_type/0,
         replicas/0]).

-define(DEFAULT_PORT, 5588).

-type cluster_type() :: local | {direct, [vg_chain_state:chain_node()]} | {srv, string()} | none.

-export_type([cluster_type/0]).

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
            logger:error("Unknown clustering option: ~p", [Other]),
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

%% Application environment values are checked by the typed accessors above.
-spec from_chain(atom(), term()) -> eqwalizer:dynamic().
from_chain(Key, Default) ->
    case application:get_env(vonnegut, chain, []) of
        [] ->
            Default;
        Chain ->
            proplists:get_value(Key, Chain, Default)
    end.

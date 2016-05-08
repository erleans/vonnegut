-module(vg).

-export([create_topic/1,
         write/2,
         get/1,
         cluster_status/0]).

create_topic(Topic) ->
    case riak_ensemble_client:kover(node(), topics, create_topic, Topic, 10000) of
        {ok, _} ->
            ok;
        Error ->
            {error, Error}
    end.

write(Topic, Message) when is_binary(Message) ->
    case riak_ensemble_client:kover(node(), topics, {write, Topic}, Message, 10000) of
        {ok, _} ->
            ok;
        Error ->
            {error, Error}
    end.

get(Topic) ->
    case riak_ensemble_client:kget(node(), topics, {Topic, 0}, 10000) of
        {ok, Value} ->
            Value;
        Error ->
            {error, Error}
    end.

%%     vg_log:write(Topic, 0, [Message]);
%% write(Topic, MessageSet) when is_list(MessageSet) ->
%%     vg_log:write(Topic, 0, MessageSet).

cluster_status() ->
    case riak_ensemble_manager:enabled() of
        false ->
            {error, not_enabled};
        true ->
            Nodes = lists:sort(riak_ensemble_manager:cluster()),
            io:format("Nodes in cluster: ~p~n",[Nodes]),
            LeaderNode = node(riak_ensemble_manager:get_leader_pid(root)),
            io:format("Leader: ~p~n",[LeaderNode])
    end.

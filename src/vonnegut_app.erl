%%%-------------------------------------------------------------------
%% @doc vonnegut public API
%% @end
%%%-------------------------------------------------------------------

-module(vonnegut_app).

-behaviour(application).

%% Application callbacks
-export([start/2,
         stop/1,
         swap_lager/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    init_tables(),
    Topics = vg_utils:topics_on_disk(),
    Partition = 0,
    lists:map(fun({Topic, _}) ->
                      TopicDir = vg_utils:topic_dir(Topic, Partition),
                      {Id, _, _} = vg_log_segments:find_latest_id(TopicDir, Topic, Partition),
                      lager:info("inserting hwm ~p ~p", [Topic, Id]),
                      vg_topics:insert_hwm(Topic, Partition, Id - 1)
              end, Topics),
    lager:info("starting supervisor", []),
    vonnegut_sup:start_link().


%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

init_tables() ->
    vg_log_segments:init_table(),
    vg_topics:init_table().

%% TODO: ifdef this out in non-test builds
swap_lager(Pid) ->
    %% our testing environment has provided us with a remote
    %% lager sink to target messages at, but we can't target
    %% it directly, so proxy message through to it.
    Proxy = spawn(fun Loop() ->
                          receive
                              E -> Pid ! E
                          end,
                          Loop()
                  end),
    Lager = whereis(lager_event),
    true = unregister(lager_event),
    case (catch register(lager_event, Proxy)) of
        true ->
            lager:info("swapped local lager_event server with: ~p", [Pid]);
        Other ->
            register(lager_event, Lager),
            lager:info("noes we failed: ~p", [Other])
    end.

-module(prop_vg).

-include_lib("proper/include/proper.hrl").

-define(MODEL, vg_statem).

prop_test() ->
    ?FORALL(Cmds, more_commands(8, commands(?MODEL)),
            begin
                lager:start(),
                lager:set_loglevel(lager_console_backend, error),
                application:ensure_all_started(vonnegut),
                {History, State, Result} = run_commands(?MODEL, Cmds),
                application:stop(vonnegut),
                ?WHENFAIL(io:format("History: ~p\nState: ~p\nResult: ~p\n",
                                    [History,State,Result]),
                          aggregate(command_names(Cmds), Result =:= ok))
            end).

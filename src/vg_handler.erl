-module(vg_handler).
-behaviour(ranch_protocol).

-export([start_link/4]).
-export([init/4]).

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, _Opts = []) ->
    ok = ranch:accept_ack(Ref),
    loop(Socket, Transport).

loop(Socket, Transport) ->
    case Transport:recv(Socket, 0, 5000) of
        {ok, Data} ->
            [Num, _] = binary:split(Data, <<"\r\n">>),
            Id = binary_to_integer(Num),
            {Filename, Position} = vg_index:find_in_index(<<"test">>, Id),
            {ok, Fd} = vg_utils:open_read(Filename),
            Position1 = vg_log:find_in_log(Fd, Id, Position),
            send_chunks(Fd, Socket, Position1),
            loop(Socket, Transport),
            file:close(Fd);
        _ ->
            ok = Transport:close(Socket)
    end.

send_chunks(Fd, Socket, Position) ->
    file:sendfile(Fd, Socket, Position, 0, []).

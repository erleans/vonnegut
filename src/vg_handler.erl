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
            io:format("Data ~p~n", [Data]),
            [Num, _] = binary:split(Data, <<"\r\n">>),
            Id = binary_to_integer(Num),
            {Filename, Position} = vg_log:read(<<"test">>, <<"0">>, Id),
            io:format("Data ~p~n", [Position]),

            {ok, Fd} = file:open(Filename, [read, binary, raw]),

            send_chunks(Fd, Socket, Position),

            loop(Socket, Transport);
        _ ->
            ok = Transport:close(Socket)
    end.

send_chunks(Fd, Socket, Position) ->
    file:sendfile(Fd, Socket, Position, 0, []).

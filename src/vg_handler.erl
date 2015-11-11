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
            MessageId = binary_to_integer(Num),
            Partition = 0,
            Topic = <<"test">>,

            {SegmentId, Position} = vg_utils:find_segment_offset(Topic, Partition, MessageId),

            {ok, [LogDir]} = application:get_env(vonnegut, log_dirs),
            TopicDir = filename:join(LogDir, [binary_to_list(Topic), "-", integer_to_list(Partition)]),
            File = vg_utils:log_file(TopicDir, SegmentId),
            {ok, Fd} = file:open(File, [read, binary, raw]),
            send_chunks(Fd, Socket, Position),
            file:close(Fd),
            loop(Socket, Transport);
        _ ->
            ok = Transport:close(Socket)
    end.

send_chunks(Fd, Socket, Position) ->
    file:sendfile(Fd, Socket, Position, 0, []).

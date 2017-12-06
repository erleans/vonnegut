-module(protocol_SUITE).

-compile(export_all).

%% imo eventually this should be a propEr test

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("include/vg.hrl").

suite() ->
    [{timetrap,{seconds,30}}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

groups() ->
    [].

all() ->
    [
     incomplete_fetch_decode,
     incomplete_produce_decode %,
     %% client_incomplete_handling
    ].

incomplete_fetch_decode(_Config) ->
    %% do we need the correlation id stuff here? or is that decoded directly?
    Topic = <<"foo">>,
    {_, EncodedSet} =
        lists:foldl(
          fun(Rec, {ID, IOL}) ->
                  {NextID, _Size, Enc} = vg_protocol:encode_log(ID, #{record => Rec}),
                  %% ct:pal("rec ~p, NextID ~p enc ~p", [Rec, NextID, Enc]),
                  {NextID, [IOL | Enc]}
          end,
          {55, []}, [<<"bar1">>, <<"bar2">>, <<"bar3">>, <<"bar4">>, <<"bar5">>]),

    FTR = vg_protocol:encode_fetch_topic_response(0, 0, 99, iolist_size(EncodedSet)),

    RespIO = [<<1:32/signed-integer>>, vg_protocol:encode_string(Topic),
              <<1:32/signed-integer>>, FTR, EncodedSet],

    ct:pal("resp ~p", [RespIO]),

    FullResponse = iolist_to_binary(RespIO),

    %% make sure that the full request is valid before we start breaking it up
    ?assertEqual(#{<<"foo">> =>
                            #{0 =>
                                  #{error_code => 0,high_water_mark => 99,
                                    record_set =>
                                        [#{crc => -1443659216,id => 55,
                                           record => <<"bar1">>},
                                         #{crc => 821744522,id => 56,
                                           record => <<"bar2">>},
                                         #{crc => 1207821084,id => 57,
                                           record => <<"bar3">>},
                                         #{crc => -644254017,id => 58,
                                           record => <<"bar4">>},
                                         #{crc => -1365359063,id => 59,
                                           record => <<"bar5">>}],
                                    record_set_size => 150}}},
                 vg_protocol:decode_fetch_response(FullResponse)),

    [begin
         Head = binary:part(FullResponse, 0, N),
         ?assertEqual(more, vg_protocol:decode_fetch_response(Head))
     end
     || N <- lists:seq(1, byte_size(FullResponse) - 1)],

    ok.

incomplete_produce_decode(_Config) ->
    Topic = <<"foo">>,
    Partition = 0,
    Results = [{Topic, [{Partition, 0, 444}]}],
    %% not sure why it won't use the macro here
    %% Results = [{Topic, [{Partition, ?NO_ERROR, 444}]}],
    ProduceResponse0 = vg_protocol:encode_produce_response(Results),
    ProduceResponse = iolist_to_binary(ProduceResponse0),
    ?assertEqual(#{<<"foo">> =>
                            #{0 => #{error_code => 0,offset => 444}}},
                 vg_protocol:decode_response(?PRODUCE_REQUEST, ProduceResponse)),

    [begin
         Head = binary:part(ProduceResponse, 0, N),
         ?assertEqual(more, vg_protocol:decode_response(?PRODUCE_REQUEST, Head))
     end
     || N <- lists:seq(1, byte_size(ProduceResponse) - 1)],

    ok.

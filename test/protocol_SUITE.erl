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
                  #{last_offset_delta := L,
                    record_batch := RecordBatch} = vg_protocol:encode_record_batch(Rec),
                  {ID+L+1, [IOL | [<<ID:64/signed-integer, (iolist_size(RecordBatch)):32/signed-integer>>, RecordBatch]]}
          end,
          {55, []}, [<<"bar1">>, <<"bar2">>, <<"bar3">>, <<"bar4">>, <<"bar5">>]),

    FTR = vg_protocol:encode_fetch_topic_response(0, 0, 99, iolist_size(EncodedSet)),

    RespIO = [<<1:32/signed-integer>>, vg_protocol:encode_string(Topic),
              <<1:32/signed-integer>>, FTR, EncodedSet],

    ct:pal("resp ~p", [RespIO]),

    FullResponse = iolist_to_binary(RespIO),

    %% make sure that the full request is valid before we start breaking it up
    ?assertMatch(#{<<"foo">> :=
                            #{0 :=
                                  #{error_code := 0,high_water_mark := 99,
                                    record_batches :=
                                        [#{offset := 55,
                                           value := <<"bar1">>},
                                         #{offset := 56,
                                           value := <<"bar2">>},
                                         #{offset := 57,
                                           value := <<"bar3">>},
                                         #{offset := 58,
                                           value := <<"bar4">>},
                                         #{offset := 59,
                                           value := <<"bar5">>}],
                                    record_batches_size := 355}}},
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

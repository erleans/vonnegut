-module(vg_test_utils).

-compile(export_all).

create_random_name(Name) ->
    <<Name/binary, "-", (erlang:integer_to_binary(rand:uniform(1000000)))/binary>>.

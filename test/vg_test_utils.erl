-module(vg_test_utils).

-compile(export_all).

create_random_name(Name) ->
    <<Name/binary, "-", (erlang:integer_to_binary(crypto:rand_uniform(0, 1000000)))/binary>>.

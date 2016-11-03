vonnegut
=====


Build
-----

```
$ rebar3 compile
```

Erlang Interface
---

```
$ rebar3 shell

1> vg:create_topic(<<"test_topic">>).
2> vg:write(<<"test_topic">>, <<"some log message">>).
```

By default index and log files will be written to `./data`:

```
$ ls data/test_topic-0/
00000000000000000000.index  00000000000000000000.log
```

Kafkaesque Client
---

```
> vg_client_pool:start().
ok
> vg_client:produce(<<"my-topic">>, [<<"message 1">>, <<"message 2">>]).
[{<<"my-topic">>,[{0,0,2}]}]
> vg_client:fetch(<<"my-topic">>).
[<<"message 1">>, <<"message 2">>]
```

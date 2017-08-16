vonnegut
=====

[![CircleCI](https://circleci.com/gh/SpaceTime-IoT/vonnegut.svg?style=svg)](https://circleci.com/gh/SpaceTime-IoT/vonnegut)

[![Coverage Status](https://coveralls.io/repos/github/SpaceTime-IoT/vonnegut/badge.svg?branch=master)](https://coveralls.io/github/SpaceTime-IoT/vonnegut?branch=master)

Build
-----

```shell
$ rebar3 compile
```

Running Tests
-----

The tests require opening thousands of files and so may require increasing the limit per process on your system with:

```shell
$ ulimit -n 63536
```

Tests also require a nodename:

```shell
$ rebar3 ct --name=testrunner@127.0.0.1
```

Erlang Interface
---

```shell
$ rebar3 shell
1> vg:create_topic(<<"test_topic">>).
2> vg:write(<<"test_topic">>, <<"some log message">>).
```

By default index and log files will be written to `./data`:

```shell
$ ls data/test_topic-0/
00000000000000000000.index  00000000000000000000.log
```

Kafkaesque Client
---

```erlang
> vg_client_pool:start().
ok
> vg_client:produce(<<"my-topic">>, [<<"message 1">>, <<"message 2">>]).
[{<<"my-topic">>,[{0,0,2}]}]
> vg_client:fetch(<<"my-topic">>).
[<<"message 1">>, <<"message 2">>]
```

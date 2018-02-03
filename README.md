vonnegut
=====

[![CircleCI](https://circleci.com/gh/SpaceTime-IoT/vonnegut.svg?style=svg)](https://circleci.com/gh/SpaceTime-IoT/vonnegut)

[![Coverage Status](https://coveralls.io/repos/github/SpaceTime-IoT/vonnegut/badge.svg?branch=master)](https://coveralls.io/github/SpaceTime-IoT/vonnegut?branch=master)

Vonnegut is a append-only log that follows the file format and API of Kafka 1.0. The server can be run standalone, with 1 or more chains each with 1 or more replicas, or as part of another Erlang release which can talk to it directly.

Each chain is responsible for a range of the topic space. A read or write to a topic requires finding what chain the topic belongs to and then making a request to the head, in the case of a write, or the tail, in the case of a read.

Configuration
-----

### Server

A node in a chain can discover other nodes within the chain through DNS SRV record queries. The `replicas` configuration tells vonnegut node how many other nodes it needs to connect to to form the required chain length to ack writes.

```
{vonnegut, [{chain, [{name, "chain-1"},
                     {discovery, {srv, "chain-1.service.cluster.local"}},
                     {replicas, "2"},
                     {port, 5555}]}
            ]}
```

### Client

Clients start a pool of connections to the head and tail of each chain. Chains are found through DNS queries against endpoints:

```
{vonnegut, [{client, [{endpoints, [{"chain-1.service.cluster.local", 5555}]}]}]}
```

Erlang Interface
---

A local interface can be used to create, read and write topics. 

```shell
$ rebar3 shell
1> vg:create_topic(<<"test_topic">>).
2> vg:write(<<"test_topic">>, [<<"some log message">>, <<"more log message">>]).
3> vg:fetch(<<"test_topic">>).
{ok,#{high_water_mark => 1,partition => 0,
      record_batches =>
          [#{headers => [],key => <<>>,offset => 1,sequence_number => 1,
             timestamp => 1517613646458,value => <<"more log message">>},
           #{headers => [],key => <<>>,offset => 0,sequence_number => 0,
             timestamp => 1517613646458,
             value => <<"some log message">>}]}}
```

By default index and log files will be written to `./data`:

```shell
$ ls data/test_topic-0/
00000000000000000000.index  00000000000000000000.log
```

Kafkaesque Client
---

```erlang
$ rebar3 shell
1> vg_client_pool:start().
ok
2> vg_client:produce(<<"my-topic-2">>, [<<"message 1">>, <<"message 2">>]).
{ok,1}
3> vg_client:fetch(<<"my-topic-2">>).
{ok,#{<<"test_topic-2">> =>
          #{0 =>
                #{error_code => 0,high_water_mark => 1,
                  record_batches =>
                      [#{headers => [],key => <<>>,offset => 1,
                         sequence_number => 1,timestamp => 1517616861441,
                         value => <<"message 2">>},
                       #{headers => [],key => <<>>,offset => 0,
                         sequence_number => 0,timestamp => 1517616861441,
                         value => <<"message 1">>}],
                  record_batches_size => 95}}}}
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

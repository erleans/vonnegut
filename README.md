vonnegut
=====

[![CI](https://github.com/erleans/vonnegut/actions/workflows/ci.yml/badge.svg)](https://github.com/erleans/vonnegut/actions/workflows/ci.yml)

[![codecov](https://codecov.io/gh/erleans/vonnegut/branch/master/graph/badge.svg)](https://codecov.io/gh/erleans/vonnegut)

Vonnegut is a append-only log that follows the file format and API of Kafka 1.0. The server can be run standalone, with 1 or more chains each with 1 or more replicas, or as part of another Erlang release which can talk to it directly.

Each chain is responsible for a range of the topic space. A read or write to a topic requires finding what chain the topic belongs to and then making a request to the head, in the case of a write, or the tail, in the case of a read.

Building
-----

Use Erlang/OTP 29 and rebar3 3.27.0 (versions are pinned in `.tool-versions`).
Native test dependencies require a C/C++ compiler, CMake, and make.

```shell
$ rebar3 compile
$ rebar3 as prod tar
```

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

Start the distribution port mapper before running the tests. The test runner
uses the long node name `testrunner@127.0.0.1`; cluster tests start local nodes
with OTP’s `peer` module:

```shell
$ epmd -daemon
$ rebar3 do ct --name=testrunner@127.0.0.1, cover
$ rebar3 covertool generate
```

GitHub Actions runs compilation, xref, Dialyzer, ELP Eqwalizer, Common Test, and
release packaging on OTP 29. Test logs and coverage reports are retained as
workflow artifacts. Coverage is uploaded to Codecov using the repository’s
`CODECOV_TOKEN` secret.

Application modules enforce specs for every function and types for every record
field at compile time. Run both type checkers locally with:

```shell
$ rebar3 dialyzer
$ elp eqwalize-all --as default --bail-on-error --stats
```

CI pins ELP to `2026-08-10` (the OTP 29 build). `.elp.toml` enables checking
across the application. `eqwalizer_support` provides the checker’s library
specifications; it is not started by the release.

Application logging uses OTP’s `logger` API. Lager remains for Partisan and
legacy test logging. OpenCensus and its Google reporter are no longer included.
Optional Snappy and LZ4 decoding still requires the corresponding codec modules
to be supplied by the embedding release.

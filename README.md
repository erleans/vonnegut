vonnegut
=====


Build
-----

```
$ rebar3 compile
```

Run
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

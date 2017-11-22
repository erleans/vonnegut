FROM tsloughter/erlang-alpine:20.0.1 as builder

WORKDIR /usr/src/app
COPY . /usr/src/app

RUN rebar3 as prod tar

RUN mkdir -p /opt/rel
RUN tar -zxvf /usr/src/app/_build/prod/rel/*/*.tar.gz -C /opt/rel

FROM alpine:3.6

RUN apk add --no-cache openssl-dev ncurses

WORKDIR /opt/vonnegut

ENV RELX_REPLACE_OS_VARS true
ENV NODE 127.0.0.1
ENV COOKIE vonnegut
ENV CHAIN_NAME chain1
ENV REPLICAS 1
ENV PEER_IP 127.0.0.1
ENV DISCOVERY_DOMAIN local

COPY --from=builder /opt/rel /opt/vonnegut

EXPOSE 5555 5555

ENTRYPOINT ["/opt/vonnegut/bin/vonnegut"]

CMD ["foreground"]

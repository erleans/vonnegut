FROM erlang:29.0.6 AS builder

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends build-essential git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app
COPY . /usr/src/app

RUN rebar3 as prod tar

RUN mkdir -p /opt/rel
RUN tar -zxvf /usr/src/app/_build/prod/rel/*/*.tar.gz -C /opt/rel

FROM debian:trixie-slim

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       ca-certificates libncurses6 libsctp1 libstdc++6 openssl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/vonnegut

ENV RELX_REPLACE_OS_VARS=true
ENV NODE=127.0.0.1
ENV COOKIE=vonnegut
ENV CHAIN_NAME=chain1
ENV REPLICAS=1
ENV PEER_IP=127.0.0.1
ENV DISCOVERY_DOMAIN=local

COPY --from=builder /opt/rel /opt/vonnegut

EXPOSE 5555 8000

ENTRYPOINT ["/opt/vonnegut/bin/vonnegut"]

CMD ["foreground"]

FROM ubuntu:16.04

RUN apt-get update && \
    apt-get install --no-install-recommends -y libssl-dev && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/vonnegut

ENV RELX_REPLACE_OS_VARS true

ADD ./vonnegut.tar.gz /opt/vonnegut

EXPOSE 8080 8080

ENTRYPOINT ["/opt/vonnegut/bin/vonnegut"]

CMD ["foreground"]

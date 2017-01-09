#!/usr/bin/env bash

set -xe

rebar3 as prod tar

mv _build/prod/rel/vonnegut/vonnegut-*.tar.gz ./

VERSION=$(ls vonnegut-*.tar.gz | sed -n 's/vonnegut-\(.*\).tar.gz/\1/p')
VERSION=${VERSION/+/-}

mv vonnegut-$VERSION.tar.gz vonnegut.tar.gz

docker build --rm=false -t us.gcr.io/nucleus-sti/vonnegut:$VERSION .

docker push us.gcr.io/nucleus-sti/vonnegut:$VERSION

rm vonnegut.tar.gz

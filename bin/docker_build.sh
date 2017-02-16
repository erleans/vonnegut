#!/usr/bin/env bash

set -xe

if [[ $1 ]]; then
    REPO=$1
else
    REPO=us.gcr.io/nucleus-sti
fi

rebar3 as prod tar

mv _build/prod/rel/vonnegut/vonnegut-*.tar.gz ./

VERSION=$(ls vonnegut-*.tar.gz | sed -n 's/vonnegut-\(.*\).tar.gz/\1/p')
VERSION=${VERSION/+/-}

mv vonnegut-$VERSION.tar.gz vonnegut.tar.gz

docker build --rm=false -t vonnegut:$VERSION .

if [[ $REPO ]]; then
    docker tag vonnegut:$VERSION $REPO/vonnegut:$VERSION
    docker push $REPO/vonnegut:$VERSION
fi

rm vonnegut.tar.gz

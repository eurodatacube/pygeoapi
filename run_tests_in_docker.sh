#!/usr/bin/env bash

#
# Start test run script in docker.
# Args are passed to pytest-watch, or bash is started if $1 is bash
#

set -euxo pipefail

docker build . -t mygeoapi:1

docker run --rm -it -v `pwd`:/pygeoapi -v `pwd`/docker/default.config.yml:/pygeoapi/local.config.yml --entrypoint /pygeoapi/run_tests.sh --user 0  mygeoapi:1 $@

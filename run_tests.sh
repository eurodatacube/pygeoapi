#!/usr/bin/env bash

# Use in docker (usually by calling run_tests_in_docker.sh)

set -euxo pipefail

apt update && apt install python3-setuptools
python3 -m pip install pytest-watch

if [ "$1" = "bash" ] ; then
    exec bash
else
    exec ptw $@
fi

#!/usr/bin/env bash

set -eux

O=`tempfile`

echo "writing to $O"

ID=$1
curl -v -X POST \
    --data-raw '{"inputs": {
        "factor": {"value": 1.2},
        "other_var": {"value": [3, 4]}
    }}' \
    -H "Content-Type: application/json"    \
    "https://edc-oapi.dev.hub.eox.at/oapi/processes/${ID}/coverage?f=GeoTIFF&subset=lat(16:16.001),lon(48:48.001),time(%222020-09-10T00:00Z%22:%222020-09-29T00:00Z%22)&rangeSubset=ndvi"  > $O



echo

echo "Written to $O"
ls -l $O
file $O
rio info $O

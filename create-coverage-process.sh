#!/usr/bin/env bash

set -eux

ID=$1
curl -v -X POST \
    "https://edc-oapi.dev.hub.eox.at/oapi/processes/" \
    -H "Content-Type: application/json" \
    --data-raw '{
        "id": "'${ID}'",
        "inputs": {
            "data" : [
                {
                    "collection": "https://edc-oapi.hub.eox.at/oapi/collections/S2L2A"
                }
            ],
            "sourceBands" : [ { "value" : [ "B04", "B08" ] } ],
            "bandsPythonFunctions" : {
                "value" : {
                    "ndvi" : "return (ds[0].B08 - ds[0].B04 ) / (ds[0].B04 + ds[0].B08)",
                    "b4" : "return (ds[0].B04)"
                }
            }
        }
    }'


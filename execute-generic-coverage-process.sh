#!/usr/bin/env bash

set -x
O=`tempfile coverage-process`

echo "writing to $O"

curl -v -X POST \
    "https://edc-oapi.dev.hub.eox.at/oapi/processes/generic/coverage?f=GeoTIFF&subset=lat(16:16.001),lon(48:48.001),time(%222020-09-10T00:00Z%22:%222020-09-29T00:00Z%22)&rangeSubset=b4"  \
    -H "Content-Type: application/json" \
    --data-raw '{
        "process": "https://edc-oapi.hub.eox.at/oapi/processes/python-coverage-processor",
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
    }' > $O


echo "wrote to $O"

ls -l $O
file $O
rio info $O

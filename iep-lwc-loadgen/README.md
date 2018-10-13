
## Description

Service for generating load against a service running the Atlas LWCAPI. It is primarly used
for helping to test new versions of the service and the eval client with a configured set
of load to ensure there are no performance regressions.

## Usage

Update the `iep.lwc.loadgen.uris` config setting with a list of Atlas URIs to stream. Example:

```
iep.lwc.loadgen.uris = [
  "http://localhost:7101/api/v1/graph?q=name,m1,:eq,:avg",
  "http://localhost:7101/api/v1/graph?q=name,m2,:eq,:sum",
  "http://localhost:7101/api/v1/graph?q=name,m3,:eq,:sum&step=10s"
]
```

The default step size is 60s. It can be customized for a given URI by using the `step` query
parameter.

## Clone Existing Deployment

To copy the expression set from an existing LWCAPI deployment, the following script can be
adapted to generate the config:

```bash
#!/bin/bash

LWC_HOST=localhost:7101
ATLAS_HOST=localhost:7102
CONF_FILE=iep-lwc-loadgen/src/main/resources/custom.conf

curl -s "http://$LWC_HOST/lwc/api/v1/expressions" |
  jq --arg host "$ATLAS_HOST" '
    .expressions[] |
    @text "http://\($host)/api/v1/graph?q=\(.expression)&step=\(.frequency / 1000)s"' |
  awk '
    BEGIN { print "iep.lwc.loadgen.uris = ["}
    { print "  " $0 "," }
    END { print "]" }
  ' > $CONF_FILE
```


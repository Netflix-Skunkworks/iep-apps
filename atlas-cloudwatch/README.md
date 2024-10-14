# Description

This module handles ingesting CloudWatch metrics into Atlas. It is designed to accept
data from a CloudWatch metrics stream over a Firehose stream to an HTTP endpoint as well
as poll a set of accounts and regions for data that is not available via the Firehose
stream. Due to inconsistencies in CloudWatch data, values are cached on receipt and a
separate process scrapes the cache to publish data downstream.

Data from CloudWatch can include:

* Out of order values
* Variably delayed values (from a minute to multiple minutes)
* Various step sizes (1m for most, 5m for some, daily for others)
* Raw counters needing conversion to rates
* Batch updates with rolling windows

Due to these differences, a fair amount of manual tuning per namespace, and sometimes per metric,
is needed.

# Namespace Configurations

Configurations are Hocon Typesafe config files located in the [resources](src/main/resources)
directory. Each AWS CloudWatch namespace should have a file. Sub namespaces may be included in
the parent file. Configs appear under the `atlas.cloudwatch` prefix. Configuration entries
have the following format:

```hocon

elb = {
  namespace = "AWS/ELB"
  period = 1m,
  period-count = 6
  end-period-offset = 9
  timeout = 15m
  filter = ":false"
  poll-offset = 7h

  dimensions = [
    "LoadBalancerName",
    "AvailabilityZone"
  ]

  metrics = [
    {
      name = "HTTPCode_ELB_4XX"
      alias = "aws.elb.errors"
      conversion = "sum,rate"
      tags = [
        {
          key = "status"
          value = "4xx"
        }
      ]
    }
  ]
}
```

The key is a name for the category, in this case `elb`. Each key must be unique across the
entire config directory. Fields under the key include:

* `namespace` refers to the fully qualified AWS namespace the metrics appear under. This can be
  found in CloudWatch, documentation or app metrics.
* `period` is how frequently values are posted for the metric. Most arrive every minute though
  some arrive every 5m or 1d. Atlas duration format. See [Configuring Period](#configuring-period)
* `period-count` is how many `period`s of data to keep in the cache. A larger `end-period-offset`
  overrides the period count. See [Configuring Period Count](#configuring-period-count)
* `end-period-offset` refers to how many `period`s prior to the cache scrape timestamp to look for
  a value. CloudWatch data is often delayed by multiple minutes and sometimes data is updated.
  See [Configuring End Period Offset](#configuring-end-period-offset)
* `timeout` allows a `0` value to be posted for the timeout period if a stale value is present in
  the cache instead of dropping the data immediately. Useful for sparse data. Atlas duration format.
  See [Configuring Timeouts](#configuring-timeouts)
* `filter` an ASL query that will drop any data that matches.
* `poll-offset` indicates the data must be polled instead of expected via Firehose. Used for daily
  metrics like S3 bucket aggregates or those with timestamps 2 hours older than wall clock or high resolutions metrics.
* `dimensions` a list of zero or more dimensions to match on incoming CloudWatch data. See
  [Configuring Dimensions](#configuring-dimensions)
* `metrics` a list of one or more metric conversion definitions.

## Configuring Period

Most metrics post every minute (with some delay). Some post every 5 minutes, others
once an hour or once a day. Set the `period` to the proper value after reading docs,
checking the console or experimenting. Use the debugger to capture a CloudWatch metric, then
plot the `dist-avg` of `atlas.cloudwatch.dbg.scrape.step` in Atlas to find the step.

## Configuring Period Count

The `period-count` setting determines how many windows of data to maintain in the
cache within the expiration window. The default is `6` windows, meaning that if the period is
`1m`, there would be up to 6 minutes of data in the cache at any time. In general, the `end-period-offset`
will handle period count situations and this may be used for debugging by setting a very wide
window and using the `atlas.cloudwatch.dbg.scrape.past` Atlas metric to determine the proper
offset to configure.

## Configuring Dimensions

The `dimensions` specified must match the combination of tags desired to be persisted in Atlas.
Firehose will publish various rollups of the data so any data points that have more or fewer tags
than the configuration will be dropped. The dimensions may be verified in documentation or by
checking the groupings in the AWS Console. Within some AWS metrics namespaces, the different
metrics have different dimensions, so you may need to break up the definition of metrics.

**NOTE**
Be careful when defining configurations with multiple dimension sets for the same data. Cloudwatch has
multiple aggregates. Some metrics are emitted with a standard set of dimensions and users can opt-in to
a set of more detailed metrics that include extra dimensions. Generally, choose the standard set and
add the detailed set if users request.

See [route53.conf](src/main/resources/route53.conf) as an example.

## Configuring End Period Offset

The `end-period-offset` setting determines how far back from the current scrape timestamp
(normalized to the top of the minute) to look for data to publish. The default is `1` interval,
meaning data for the previous minute will be returned (if present) even if there is a value for
the current minute. Most CloudWatch data is delayed at least one period, most for multiple
periods.

Some may be delayed for longer periods _or_ the data may not be completed until later. Data often
arrives out of order as well. In these cases, increase the `end-period-offset` so that accurate
information can be processed. If data looks to be jagged and should be consistent, per CloudWatch,
try adjusting the offset period back in time.

To find the proper offset, you can usually use the `dist-avg` and `dist-max` of the
`atlas.cloudwatch.dbg.scrape.past` metric from the debugger. The maximum is often the safest value
to use but increases the delay and usefulness of the metric. Try the average first and if the data
fails to match what is reported in the CloudWatch UI, try the max.

Wait an hour or so after deployment or adding a new firehose stream before analyzing delays as
the firehose will post historic data and make it look like data is older than it is.

Data in the cache earlier than the offset will not be published. Data newer than the offset
will be published.

A stair-step pattern and nothing showing up in the scrape past metric can mean that the value
is set too far in the past so try reducing the offset.

**Note**
Published data timestamps are always the timestamp of the scrape interval, regardless of the
actual CloudWatch data point timestamp.

## Configuring Timeouts

Since Netflix metrics infrastructure expects constantly reporting data, timeouts can be configured
that will keep publishing a `0` value if no new values are present at or before the `end-period-offset`
until the timeout period elapses. E.g. for data with a 5 minute `period`, setting a timeout of 5m
would publish a value of `0` every minute until a new value arrives or 5 minutes have elapsed.

This is useful for sparse data that may plot as a single data point in a graph. Setting a timeout
will print a vertical line to make the values obvious.

## Configuring Conversions

See [Conversions](src/main/scala/com/netflix/atlas/cloudwatch/Conversions.scala) and
[MetricDefinition](src/main/scala/com/netflix/atlas/cloudwatch/MetricDefinition.scala) for details.

The Atlas DS type is chosen based on the conversion. Anything that has a rate conversion will use a
rate (e.g. rate-per-second), otherwise it will be treated as a gauge.

The first element of a conversion is the statistic to extract from the CloudWatch metric. There are
five base types and three composite types. Allowed values are:

* Base Types
  * `avg`
  * `count`
  * `max`
  * `min`
  * `sum`
* Composite Types
  * `dist-summary`
  * `timer`
  * `timer-millis`

A base type statistic may be followed by an optional rate conversion, e.g., `sum,rate`. Allowed
values are:

* `percent`
* `rate`

The `percent` conversion will multiply the value by 100, to represent a percentage value.

The `rate` conversion will divide the value by the sample period from the cloudwatch metadata
to get a rate-per-second for the value. The rate conversion is unit aware, so if the unit for
the datapoint is  already a rate, then no conversion will take place.

In addition to the conversions specified by name, a unit conversion will automatically be applied
to the data. This ensures that a base unit is reported.

The composite statistic types will get mapped in as close as possible to the way spectator would
map in those types. The primary gap is that, with CloudWatch, we cannot get a totalOfSquares, so
the standard deviation cannot be computed.

Note, `timer` should generally be preferred over `timer-millis`. If the unit is specified on the
datapoint, then it will automatically convert to seconds. When using `timer-millis`, the unit is
ignored and the data will be assumed to be in milliseconds. It is mostly intended for use with
some latency values that do not explicitly mark the unit.

Metrics can be marked as `monotonic` if the values are raw counters. This will convert them to
a rate prior to publishing.

# Publishing

Data from the cache is scraped once a minute. The normalized timestamp of the scrape
interval will be used for the data point timestamp, regardless of the CloudWatch
timestamp or `end-period-offset`. Note that with an `end-period-offset` greater than 1,
data will not appear in Atlas as reported. There will be a constant offset.

## Routing

Settings under `atlas.cloudwatch.routing` are used to route metrics for various accounts and 
regions to the proper endpoint. Each unique account, region and endpoint will have a queue with
default settings. See [application.conf](src/test/resources/application.conf) for an example.

# Polling

Polling is used for fetching metrics that cannot be published from CloudWatch. As of this writing,
CloudWatch streams only publish data with timestamps within the past 2 hours of the wall-clock
time. Some metrics, like S3 bucket aggregates, are computed once a day with timestamps back-dated
to midnight. Therefore the `poll-offset` can be set on a category to request the data from CloudWatch
once a day. This saves on request costs. Polled data is then sent to the metric cache and published
in the same way as Firehose data.

# Troubleshooting

## Debugger

A [debugger](src/main/scala/com/netflix/atlas/cloudwatch/CloudWatchDebugger.scala) exists
to capture data as it flows through the application. Data is logged and debug metrics are
reported. Via config under `atlas.cloudwatch.debug` there are various flags and filters
that can be applied to the stream, scraper and poller. Flags like `dropped.tags` will
log an **example**, each minute, of a message that fails to match the dimensions listed
for a namespace category.

ASL queries can be supplied via `filters` that will match any message regardless of state
at all stages in processing. Note that the ASL is applied _before_ final tag conversion, so
it operates on the raw CloudWatch data. Since ASL doesn't allow all characters, there may be
a slight mismatch between what could be detected. The CloudWatch metric name will map to
the `name` key as in Atlas while the AWS CloudWatch namespace maps to the `aws.namespace` key.
Note that AWS namespaces hava a forward slash like `AWS/EC2` and the slash is replaced with a
`_` for ASL matching. E.g. `aws.namespace,AWS_S3,:eq,name,(,BucketSizeBytes,NumberOfObjects,),:in,:and`
will match all S3 BucketSizeBytes and NumberOfObjects metrics.

## Missing metric data

* Verify the namespace is listed in a config file. You should see `atlas.cloudwatch.datapoints.filtered`
  with a `reason = aws.namespace` at greater than zero.
* Verify the category for metric is loaded in the `reference.conf` or `application.conf` files.
* Check for typos in the metric and namespace. Particularly if `atlas.cloudwatch.datapoints.filtered`
  with `reason` of `metric` or `aws.namespace` may be incrementing.
* Validate the tag names match what can be plotted in CloudWatch. The tags much match
  exactly. The `atlas.cloudwatch.datapoints.filtered` metric with a `reason = tags` will be incremented.
* If the rest looks ok, add a set `atlas.cloudwatch.debug.scrape.stale` to `true` and check
  the `atlas.cloudwatch.dbg.scrape.past` metric for the namespace and AWS metric to find the
  average and max offset. Then set the `end-period-offset` to a good value.
* Add a debugger entry to see exactly what CloudWatch is sending.
  * If the data never shows in the debugger, double check if the metric is reported every day
    or greater than every 2 hours. CloudWatch currently posts data with timestamps within the
    last 2 hours (wall clock) so daily data (like S3 bucket sizes and object counts) must be
    scraped rather than streamed.

## Values differ from CloudWatch

* Follow the steps above to record the `atlas.cloudwatch.dbg.scrape.past` metric and look at
  the max and average values.
  * If nothing is recorded, it could be that the offset is too old and that may cause the
    value to be too high or flat. Set a smaller `end-period-offset` and tune until the values
    match CloudWatch.
  * If the offset is greater than the average and/or max, try increasing the `end-period-offset`.
* Verify the proper [Conversion](#configuring-conversions) is set for the metric type.

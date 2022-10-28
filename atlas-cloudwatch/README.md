
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

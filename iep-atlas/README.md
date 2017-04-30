
## Description

Example for running [Atlas](https://github.com/Netflix/atlas/) and:

* Having it register with [Eureka](https://github.com/Netflix/eureka/) using the
    [iep-module-eureka](https://github.com/Netflix/iep/tree/master/iep-module-eureka)
    library.
* Report metrics to itself using
    [iep-module-atlas](https://github.com/Netflix/iep/tree/master/iep-module-atlas).
* Configure for debugging using
    [iep-module-jmxport](https://github.com/Netflix/iep/tree/master/iep-module-jmxport) and
    [iep-module-admin](https://github.com/Netflix/iep/tree/master/iep-module-admin).
* Package the application for debian. These steps can be generalized for other
    systems.

The end result should look like:

![Overview](images/overview.png)

## Usage

To run the example:

```
$ sbt iep-atlas/run
```

By default it will disable the Eureka registration. To enable, modify
[eureka-client.properties][eureka-config] to talk to your Eureka instance. For
more details see the [Eureka documentation][eureka-docs].

[eureka-config]: https://github.com/Netflix-Skunkworks/iep-apps/blob/master/iep-atlas/src/main/resources/eureka-client.properties
[eureka-docs]: https://github.com/Netflix/eureka/wiki/Configuring-Eureka

## Packaging

For Debian/Ubuntu:

```
$ sbt iep-atlas/debian:packageBin
```


## Description

Example for running [Atlas](https://github.com/Netflix/atlas/) and:

* Report metrics to itself using
    [iep-spring-atlas](https://github.com/Netflix/iep/tree/main/iep-spring-atlas).
* Configure for debugging using
    [iep-spring-admin](https://github.com/Netflix/iep/tree/main/iep-spring-admin).
* Package the application for debian. These steps can be generalized for other
    systems.

The end result should look like:

![Overview](images/overview.png)

## Usage

To run the example:

```
$ sbt iep-atlas/run
```

## Packaging

For Debian/Ubuntu:

```
$ sbt iep-atlas/debian:packageBin
```

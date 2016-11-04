
## Description

Example for running [Atlas](https://github.com/Netflix/atlas/) and having it register
with [Eureka](https://github.com/Netflix/eureka/) using the
[iep-module-eureka](https://github.com/Netflix/iep/tree/master/iep-module-eureka)
library.

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

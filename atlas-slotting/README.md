## Introduction

Given a set of app names, the service periodically polls AWS AutoScalingGroups (ASGs) and AWS EC2
Instances, so that it can assign slot numbers to the instances, from 0 to N-1. As instances in the
ASG are replaced, they take over missing slot numbers on a first-come, first-served basis. To the
extent that instances in the ASG remain stable, so will the slot numbers.

The slotting service should only monitor apps with statically sized ASGs - it does not provide the
necessary slot number stability when dynamic scaling is configured. A monitored app can be scaled
up or down in size, as long as this is done through new ASG deployments.

Slotting information is used by Atlas in the hashing scheme which divides data between nodes in
large backend clusters. This information is one of the pre-requisites for running large Atlas
deployments.

DynamoDB is used for durable storage, so that slot numbers remain consistent between deployments
of the slotting service and instance replacements that may occur. If a DynamoDB table does not
exist in the account and region where the slotting service runs, then the service will create a
new one named after the cluster.

The service is designed to operate as single-instance deployments, one per region, where slotting
data is needed. When performing red-black deployments, the accuracy of the slotting information is
preserved with DynamoDB conditional put semantics. If an ASG requires a slotting update during a
deployment window, one instance will succeed in writing and the other will fail. The instances will
be synchronized on subsequent runs of the crawler threads, until the previous ASG is destroyed.

## Naming Convention

The following naming convention is used at Netflix for deploying AutoScalingGroups in AWS. String
matching is used to identify logically related groups of instances. For the slotting service, the
app name is used to configure the ASGs that should be polled.

```
       cluster
╭─────────┴──────────╮
foo_webapp-main-canary-v042
╰───┬────╯ ╰┬─╯ ╰─┬──╯ ╰┬─╯
   app   stack  detail  sequence
```

[Frigga] is a Java library used by Netflix for parsing ASG names into component parts and it
defines the authoritative rules for server group names. For performing this string parsing, we
use the Spectator IPC [ServerGroup] utility instead, which provides 274X more throughput, with
159X fewer allocations, as compared to Frigga in micro-benchmarks.

[frigga]: https://github.com/netflix/frigga
[ServerGroup]: https://github.com/Netflix/spectator/pull/551

## Configuration

* Establish IAM roles for the service.
    * Two roles are expected: a service role and an instance profile.
    * The instance profile should provide the following permissions:
        * `sts:AssumeRole` to the service role, in the target accounts.
        * `dynamodb:*` to the `table/atlas_slotting*` namespace.
    * The service role should provide the following permissions, for a given account:
        * `sts:AssumeRole` from the instance profile role.
        * `autoscaling:DescribeAutoScalingGroups` on all resources.
        * `ec2:DescribeInstances` on all resources. 
    * The `INSIGHT_ACCOUNT_ID` environment variable should be set to id of the account that will be crawled.
* Review [application.conf](./src/main/resources/application.conf)
    * `aws`
        * Set the crawl interval and page size for autoScaling and ec2.
        * Set the dynamodb table name, read capacity and write capacity.
    * `slotting`
        * Set the list of app names and the background thread intervals.

## API

This application is deployed with a single instance to each region and account combination. In the case of
instance failure and replacement, the service may be unavailable for up to 10 minutes. In practice, this is
a rare occurrence. Thus, users of the slotting API should cache results locally, and only update this cache
when a successful result is received.

The purpose of this deployment strategy is to ensure that results are always correct and consistent, from the
point at which slots are calculated. The instance is sized appropriately for the data and query load. 

<table>
    <tr><th>Action <th>Specification
    <tr>
        <td width="30%">List of Available Endpoints
        <td><code>GET /</code>
    <tr>
        <td width="30%">Healthcheck
        <td><code>GET /healthcheck</code>
    <tr>
        <td width="30%">List of AutoScalingGroups
        <td><code>GET /api/v1/autoScalingGroups</code>
    <tr>
        <td width="30%">All AutoScalingGroup Details
        <td><code>GET /api/v1/autoScalingGroups?verbose=true</code>
    <tr>
        <td width="30%">Single AutoScalingGroup Details
        <td><code>GET /api/v1/autoScalingGroups/:asgName</code>
    <tr>
        <td width="30%">List of Clusters
        <td><code>GET /api/v1/clusters</code>
    <tr>
        <td width="30%">Map of Clusters to Lists of AutoScalingGroup Details
        <td><code>GET /api/v1/clusters?verbose=true</code>
    <tr>
        <td width="30%">List of AutoScalingGroups Matching a Cluster
        <td><code>GET /api/v1/clusters/:clusterName</code>
    <tr>
        <td width="30%">All AutoScalingGroup Details Matching a Cluster
        <td><code>GET /api/v1/clusters/:clusterName?verbose=true</code>
</table>

## Metrics

The following Atlas metrics are published:

<table>
    <tr><th>Name <th>Tags <th>Description
    <tr>
        <td>crawl.count
        <td>id=&lt;asgs, instances&gt;
        <td>Number of asgs matching defined appNames or running instances crawled in AWS collections
    <tr>
        <td>crawl.errors
        <td>id=&lt;asgs, instances&gt;
        <td>Number of errors that occurred during AWS collection crawls
    <tr>
        <td>crawl.timer
        <td>id=&lt;asgs, instances&gt;
        <td>Time, in seconds, required to crawl the AWS collection
    <tr>
        <td>deleted.count
        <td>--
        <td>Number of items deleted from the DynamoDB table by the janitor task
    <tr>
        <td>dynamodb.errors
        <td>--
        <td>Number of errors that have occurred during DynamoDB updates
    <tr>
        <td>last.update
        <td>id=&lt;asgs, cache, instances, janitor, slots&gt;
        <td>Time, in seconds, since the last asgs crawl, cache update, instances crawl, janitor run
        or slots update
    <tr>
        <td>slots.changed
        <td>asg=$ASG_NAME
        <td>Number of ASGs where slots were changed
    <tr>
        <td>slots.errors
        <td>asg=$ASG_NAME
        <td>Number of ASGs where slot assignments failed validation checks
</table>

## Initial Synchronization

If you have an existing set of slotting information available, and you need a new deployment of the
slotting service to synchronize with it, then you can perform a one-time upload of data directly to
DynamoDB. Subsequent iterations of the slotting service will reload data from DynamoDB and continue
processing with the updated numbers.

A [lift-data.py](./src/scripts/lift-data.py) script is available, which will load slot numbers from
an Edda endpoint and put them into DynamoDB, for a given set of app names and regions. This is used
to assist with internal migration efforts at Netflix.

## Data Delay and Operational Notes

When new ASGs start and attempt to access their own slotting data, they will observe 404s from the slotting service for
a few minutes, until slot initialization is complete. This processing delay for new ASGs is expected behavior, based
upon the number of resources that must be crawled for a given account and region. It takes some time for ASGs to launch
in AWS, and then the slotting service polls the ASG and EC2 endpoints once per minute. For large accounts, ASG crawls
tend to complete in 10-15 seconds, for hundreds of ASGs, while instance crawls can range from 50-70 seconds, for many
thousands of instances. We attempt to coordinate the updates between ASGs and instances, waiting to have fresh caches
of both before calculating slots. This strategy helps to ensure that the slotting information is correct, and avoids
off-by-one errors during instance replacement activities.

Caching is implemented on the API routes, to help deal with high-volume queries for the same information from large
ASGs, but the TTL on that is  10 seconds, and thus is not a contributor to initialization delay.

A common pattern we have implemented for applications is storing slotting information in a dedicated service, which is
responsible for polling and storing data in memory. As a part of application startup, the service does not go healthy
until data has been received at least once, and cached data is only updated on 200 OK responses. If you squelch log
errors associated with HTTP 404 errors at startup until you get the first 200 OK, the delay should be manageable.

We run the slotting service as a single instance per account and region, as a means of simplifying operations and
removing the need to deal with cache coordination. If an instance is terminated and replaced, it could be 10
minutes until a new one is up and running, so client-side caching of this data is important. The slotting data is
safe, because it is stored in DynamoDB, so the new instance will pick up where the previous one left off.

## Local Development

```
# temporary aws credentials used for local testing
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."

# select the region where the aws clients will operate
export EC2_REGION="us-east-1"

# used to construct the dynamodb table name
export NETFLIX_STACK="local"
```

```
# verbose form, from project root
sbt "project atlas-slotting" test

# short form, from project root
sbt atlas-slotting/test
```

Using `sbt` to `run` the project no longer works for version > 1.5.6, due to [sbt/issues/6767](https://github.com/sbt/sbt/issues/6767).

Run the project from an IDE, setting env vars as needed.

```
curl http://localhost:7101
```
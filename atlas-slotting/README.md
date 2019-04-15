## Introduction

Given a set of app names, the service periodically polls AWS AutoScalingGroups (ASGs) and AWS EC2
Instances, so that it can assign slot numbers to the instances, from 0 to N. As instances in the
ASG are replaced, they take over missing slot numbers on a first-come, first-served basis. To the
extent that instances in the ASG remain stable, so will the slot numbers.

Slotting information is used by Atlas in the consistent hashing scheme which divides data between
nodes in large backend clusters. This information is one of the pre-requisites for running large
Atlas clusters.

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

[Frigga] is a Java library used by Spinnaker, Insight, and other teams for parsing names when
needed. Look at this library for the authoritative rules on server group names.

[frigga]: https://github.com/netflix/frigga

## Initial Synchronization

If you have an existing set of slotting information available, and you need a new deployment of the
slotting service to synchronize with it, then you can perform a one-time upload of data directly to
DynamoDB. Subsequent iterations of the slotting service will reload data from DynamoDB and continue
processing with the updated numbers.

A [lift-data.py](./src/scripts/lift-data.py) script is available, which will load slot numbers from
an Edda endpoint and put them into DynamoDB, for a given set of app names and regions. This is used
to assist with internal migration efforts at Netflix.

## Configuration

* Establish an [IAM Role](./iamrole-example.md) for the service.
* Review [application.conf](./src/main/resources/application.conf)
    * aws
        * Set the crawl interval and page size for autoScaling and ec2.
    * dynamoDb
        * Set the table name, read capacity and write capacity.
    * slotting
        * Set the list of app names and the background thread intervals.

## Metrics

The following Atlas metrics are published:

<table>
    <tr><th>Name <th>Tags <th>Description
    <tr>
        <td>crawl.count
        <td>id=asgs, instances
        <td>Number of matching items crawled in the AWS collection
    <tr>
        <td>crawl.errors
        <td>id=asgs, instances
        <td>Number of errors that occurred during collection crawls   
    <tr>
        <td>crawl.timer
        <td>id=asgs, instances
        <td>Time, in seconds, required to crawl the AWS collection
    <tr>
        <td>dynamoDb.errors
        <td>--
        <td>Number of errors that have occurred during DynamoDB updates
    <tr>
        <td>lastUpdate
        <td>id=cache, janitor, slots
        <td>Time, in seconds, since the last cache update, janitor run or slot update
    <tr>
        <td>slots.update
        <td>asg=$ASG_NAME
        <td>Number of ASGs where slots were updated
</table>

## API

### Standard

<table>
    <tr><th>Action <th>Specification
    <tr>
        <td width="30%">List of Available Endpoints
        <td><code>GET /</code>
    <tr>
        <td width="30%">Healthcheck
        <td><code>GET /healthcheck</code>
    <tr>
        <td width="30%">List of Available AutoScalingGroups
        <td><code>GET /api/v1/autoScalingGroups</code>
    <tr>
        <td width="30%">All AutoScalingGroup Details
        <td><code>GET /api/v1/autoScalingGroups?verbose=true</code>
    <tr>
        <td width="30%">Single AutoScalingGroup Details
        <td><code>GET /api/v1/autoScalingGroups/:asgName</code>
</table>

### Edda Compatibility

This API is designed to ease the transition from using Edda for slotting data. Any matrix args and
field selectors will be stripped from the request URL and standard payloads will be returned.

<table>
    <tr><th>Action <th>Specification
    <tr>
        <td width="30%">List of Available AutoScalingGroups
        <td><code>GET /api/v2/group/autoScalingGroups</code>
    <tr>
        <td width="30%">All AutoScalingGroup Details
        <td><code>GET /api/v2/group/autoScalingGroups;_expand</code>
    <tr>
        <td width="30%">Single AutoScalingGroup Details
        <td><code>GET /api/v2/group/autoScalingGroups/:asgName</code>
    <tr>
        <td width="30%">List of Available AutoScalingGroups
        <td><code>GET /REST/v2/group/autoScalingGroups</code>
    <tr>
        <td width="30%">All AutoScalingGroup Details
        <td><code>GET /REST/v2/group/autoScalingGroups;_expand</code>
    <tr>
        <td width="30%">Single AutoScalingGroup Details
        <td><code>GET /REST/v2/group/autoScalingGroups/:asgName</code>
</table>

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
sbt "project atlas-slotting" clean compile test
sbt "project atlas-slotting" run
```

```
curl http://localhost:7101/healthcheck
```

<script>
    var stripMargin = document.querySelectorAll(".stripmargin");
    stripMargin.forEach(p => p.textContent = p.textContent.replace(/^\s+\|/mg, ""))
    document.body.removeAttribute('loading');
</script>

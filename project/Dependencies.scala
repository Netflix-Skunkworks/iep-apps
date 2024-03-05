import sbt._

// format: off

object Dependencies {
  object Versions {
    val atlas      = "1.8.0-SNAPSHOT"
    val aws2       = "2.23.17"
    val iep        = "5.0.17"
    val log4j      = "2.22.1"
    val pekko      = "1.0.2"
    val pekkoHttpV = "1.0.1"
    val scala      = "2.13.12"
    val servo      = "0.13.2"
    val slf4j      = "1.7.36"
    val spectator  = "1.7.7"
    val spring     = "6.0.7"
    val avroV      = "1.11.3"

    val crossScala = Seq(scala)
  }

  import Versions._
  val avro               = "org.apache.avro" % "avro" % avroV
  val atlasCore          = "com.netflix.atlas_v1" %% "atlas-core" % atlas
  val atlasEval          = "com.netflix.atlas_v1" %% "atlas-eval" % atlas
  val atlasJson          = "com.netflix.atlas_v1" %% "atlas-json" % atlas
  val atlasPekkoTestkit  = "com.netflix.atlas_v1" %% "atlas-pekko-testkit" % atlas
  val atlasSpringEval    = "com.netflix.atlas_v1" %% "atlas-spring-eval" % atlas
  val atlasSpringPekko   = "com.netflix.atlas_v1" %% "atlas-spring-pekko" % atlas
  val atlasSpringWebApi  = "com.netflix.atlas_v1" %% "atlas-spring-webapi" % atlas
  val atlasWebApi        = "com.netflix.atlas_v1" %% "atlas-webapi" % atlas
  val aws2AutoScaling    = "software.amazon.awssdk" % "autoscaling" % aws2
  val aws2CloudWatch     = "software.amazon.awssdk" % "cloudwatch" % aws2
  val aws2Config         = "software.amazon.awssdk" % "config" % aws2
  val aws2DynamoDB       = "software.amazon.awssdk" % "dynamodb" % aws2
  val aws2EC2            = "software.amazon.awssdk" % "ec2" % aws2
  val aws2S3             = "software.amazon.awssdk" % "s3" % aws2
  val aws2SQS            = "software.amazon.awssdk" % "sqs" % aws2
  val caffeine           = "com.github.ben-manes.caffeine" % "caffeine" % "3.1.8"
  val frigga             = "com.netflix.frigga" % "frigga" % "0.26.0"
  val iepDynConfig       = "com.netflix.iep" % "iep-dynconfig" % iep
  val iepLeaderApi       = "com.netflix.iep" % "iep-leader-api" % iep
  val iepService         = "com.netflix.iep" % "iep-service" % iep
  val iepSpring          = "com.netflix.iep" % "iep-spring" % iep
  val iepSpringLeaderDDb = "com.netflix.iep" % "iep-spring-leader-dynamodb" % iep
  val iepSpringLeaderRds = "com.netflix.iep" % "iep-spring-leader-redis-cluster" % iep
  val iepSpringAdmin     = "com.netflix.iep" % "iep-spring-admin" % iep
  val iepSpringAtlas     = "com.netflix.iep" % "iep-spring-atlas" % iep
  val iepSpringAws2      = "com.netflix.iep" % "iep-spring-aws2" % iep
  val iepSpringJmx       = "com.netflix.iep" % "iep-spring-jmxport" % iep
  val iepSpringLeader    = "com.netflix.iep" % "iep-spring-leader" % iep
  val jedis              = "redis.clients" % "jedis" % "5.1.0"
  val jsonSchema         = "com.github.java-json-tools" % "json-schema-validator" % "2.2.14"
  val jsr305             = "com.google.code.findbugs" % "jsr305" % "3.0.2"
  val log4jApi           = "org.apache.logging.log4j" % "log4j-api" % log4j
  val log4jCore          = "org.apache.logging.log4j" % "log4j-core" % log4j
  val log4jJcl           = "org.apache.logging.log4j" % "log4j-jcl" % log4j
  val log4jJul           = "org.apache.logging.log4j" % "log4j-jul" % log4j
  val log4jSlf4j         = "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4j
  val mockitoCore        = "org.mockito" % "mockito-core" % "5.10.0"
  val mockitoScala       = "org.mockito" % "mockito-scala_2.13" % "1.17.29"
  val munit              = "org.scalameta" %% "munit" % "0.7.29"
  val openHFT            = "net.openhft" % "zero-allocation-hashing" % "0.16"
  val pekkoActor         = "org.apache.pekko" %% "pekko-actor" % pekko
  val pekkoHttpCaching   = "org.apache.pekko" %% "pekko-http-caching" % pekkoHttpV
  val pekkoHttpCore      = "org.apache.pekko" %% "pekko-http-core" % pekkoHttpV
  val pekkoHttpTestkit   = "org.apache.pekko" %% "pekko-http-testkit" % pekkoHttpV
  val pekkoSlf4j         = "org.apache.pekko" %% "pekko-slf4j" % pekko
  val pekkoTestkit       = "org.apache.pekko" %% "pekko-testkit" % pekko
  val protobuf           = "com.google.protobuf" % "protobuf-java" % "3.25.2"
  val scalaCompiler      = "org.scala-lang" % "scala-compiler" % scala
  val scalaLibrary       = "org.scala-lang" % "scala-library" % scala
  val scalaLogging       = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
  val scalaReflect       = "org.scala-lang" % "scala-reflect" % scala
  val servoCore          = "com.netflix.servo" % "servo-core" % servo
  val slf4jApi           = "org.slf4j" % "slf4j-api" % slf4j
  val slf4jLog4j         = "org.slf4j" % "slf4j-log4j12" % slf4j
  val slf4jSimple        = "org.slf4j" % "slf4j-simple" % slf4j
  val snappy             = "org.xerial.snappy" % "snappy-java" % "1.1.10.5"
  val spectatorApi       = "com.netflix.spectator" % "spectator-api" % spectator
  val spectatorAws2      = "com.netflix.spectator" % "spectator-ext-aws2" % spectator
  val spectatorAtlas     = "com.netflix.spectator" % "spectator-reg-atlas" % spectator
  val spectatorLog4j     = "com.netflix.spectator" % "spectator-ext-log4j2" % spectator
  val spectatorM2        = "com.netflix.spectator" % "spectator-reg-metrics2" % spectator
  val spectatorSandbox   = "com.netflix.spectator" % "spectator-ext-sandbox" % spectator
  val springContext      = "org.springframework" % "spring-context" % spring
  val typesafeConfig     = "com.typesafe" % "config" % "1.4.3"
}

// format: on

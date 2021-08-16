import sbt._

// format: off

object Dependencies {
  object Versions {
    val akka       = "2.6.15"
    val akkaHttpV  = "10.2.6"
    val atlas      = "1.7.0-SNAPSHOT"
    val aws2       = "2.16.92"
    val iep        = "3.0.2"
    val guice      = "4.1.0"
    val log4j      = "2.14.1"
    val scala      = "2.13.6"
    val servo      = "0.13.2"
    val slf4j      = "1.7.32"
    val spectator  = "0.133.0"
    val avroV      = "1.10.2"

    val crossScala = Seq(scala)
  }

  import Versions._
  val avro               = "org.apache.avro" % "avro" % avroV
  val akkaActor          = "com.typesafe.akka" %% "akka-actor" % akka
  val akkaHttpCaching    = "com.typesafe.akka" %% "akka-http-caching" % akkaHttpV
  val akkaHttpCore       = "com.typesafe.akka" %% "akka-http-core" % akkaHttpV
  val akkaHttpTestkit    = "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpV
  val akkaSlf4j          = "com.typesafe.akka" %% "akka-slf4j" % akka
  val akkaTestkit        = "com.typesafe.akka" %% "akka-testkit" % akka
  val alpakkaSqs         = "com.lightbend.akka" %% "akka-stream-alpakka-sqs" % "3.0.1"
  val atlasCore          = "com.netflix.atlas_v1" %% "atlas-core" % atlas
  val atlasEval          = "com.netflix.atlas_v1" %% "atlas-eval" % atlas
  val atlasJson          = "com.netflix.atlas_v1" %% "atlas-json" % atlas
  val atlasModuleAkka    = "com.netflix.atlas_v1" %% "atlas-module-akka" % atlas
  val atlasModuleEval    = "com.netflix.atlas_v1" %% "atlas-module-eval" % atlas
  val atlasModuleWebApi  = "com.netflix.atlas_v1" %% "atlas-module-webapi" % atlas
  val atlasWebApi        = "com.netflix.atlas_v1" %% "atlas-webapi" % atlas
  val aws2AutoScaling    = "software.amazon.awssdk" % "autoscaling" % aws2
  val aws2CloudWatch     = "software.amazon.awssdk" % "cloudwatch" % aws2
  val aws2DynamoDB       = "software.amazon.awssdk" % "dynamodb" % aws2
  val aws2EC2            = "software.amazon.awssdk" % "ec2" % aws2
  val aws2S3             = "software.amazon.awssdk" % "s3" % aws2
  val aws2SQS            = "software.amazon.awssdk" % "sqs" % aws2
  val frigga             = "com.netflix.frigga" % "frigga" % "0.25.0"
  val guiceCore          = "com.google.inject" % "guice" % guice
  val guiceMulti         = "com.google.inject.extensions" % "guice-multibindings" % guice
  val iepGuice           = "com.netflix.iep" % "iep-guice" % iep
  val iepLeaderApi      = "com.netflix.iep" % "iep-leader-api" % iep
  val iepLeaderDynamoDb = "com.netflix.iep" % "iep-leader-dynamodb" % iep
  val iepModuleAdmin     = "com.netflix.iep" % "iep-module-admin" % iep
  val iepModuleAtlas     = "com.netflix.iep" % "iep-module-atlas" % iep
  val iepModuleAws2      = "com.netflix.iep" % "iep-module-aws2" % iep
  val iepModuleJmx       = "com.netflix.iep" % "iep-module-jmxport" % iep
  val iepModuleLeader   = "com.netflix.iep" % "iep-module-leader" % iep
  val iepNflxEnv         = "com.netflix.iep" % "iep-nflxenv" % iep
  val iepService         = "com.netflix.iep" % "iep-service" % iep
  val jsonSchema         = "com.github.java-json-tools" % "json-schema-validator" % "2.2.14"
  val jsr305             = "com.google.code.findbugs" % "jsr305" % "3.0.2"
  val log4jApi           = "org.apache.logging.log4j" % "log4j-api" % log4j
  val log4jCore          = "org.apache.logging.log4j" % "log4j-core" % log4j
  val log4jJcl           = "org.apache.logging.log4j" % "log4j-jcl" % log4j
  val log4jJul           = "org.apache.logging.log4j" % "log4j-jul" % log4j
  val log4jSlf4j         = "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4j
  val scalaCompiler      = "org.scala-lang" % "scala-compiler" % scala
  val scalaLibrary       = "org.scala-lang" % "scala-library" % scala
  val scalaLogging       = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4"
  val scalaReflect       = "org.scala-lang" % "scala-reflect" % scala
  val scalatest          = "org.scalatest" %% "scalatest" % "3.2.9"
  val servoCore          = "com.netflix.servo" % "servo-core" % servo
  val slf4jApi           = "org.slf4j" % "slf4j-api" % slf4j
  val slf4jLog4j         = "org.slf4j" % "slf4j-log4j12" % slf4j
  val slf4jSimple        = "org.slf4j" % "slf4j-simple" % slf4j
  val snappy             = "org.xerial.snappy" % "snappy-java" % "1.1.8.4"
  val spectatorApi       = "com.netflix.spectator" % "spectator-api" % spectator
  val spectatorAws2      = "com.netflix.spectator" % "spectator-ext-aws2" % spectator
  val spectatorAtlas     = "com.netflix.spectator" % "spectator-reg-atlas" % spectator
  val spectatorLog4j     = "com.netflix.spectator" % "spectator-ext-log4j2" % spectator
  val spectatorM2        = "com.netflix.spectator" % "spectator-reg-metrics2" % spectator
  val spectatorSandbox   = "com.netflix.spectator" % "spectator-ext-sandbox" % spectator
  val typesafeConfig     = "com.typesafe" % "config" % "1.4.1"
}

// format: on

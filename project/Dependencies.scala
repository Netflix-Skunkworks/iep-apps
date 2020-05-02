import sbt._

// format: off

object Dependencies {
  object Versions {
    val akka       = "2.5.31"
    val akkaHttpV  = "10.1.11"
    val atlas      = "1.7.0-rc.7"
    val aws        = "1.11.752"
    val aws2       = "2.11.7"
    val iep        = "2.2.2"
    val guice      = "4.1.0"
    val log4j      = "2.13.1"
    val scala      = "2.13.1"
    val servo      = "0.12.28"
    val slf4j      = "1.7.30"
    val spectator  = "0.104.0"
    val avroV      = "1.9.2"

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
  val alpakkaSqs         = "com.lightbend.akka" %% "akka-stream-alpakka-sqs" % "1.1.2"
  val atlasEval          = "com.netflix.atlas_v1" %% "atlas-eval" % atlas
  val atlasJson          = "com.netflix.atlas_v1" %% "atlas-json" % atlas
  val atlasModuleAkka    = "com.netflix.atlas_v1" %% "atlas-module-akka" % atlas
  val atlasModuleEval    = "com.netflix.atlas_v1" %% "atlas-module-eval" % atlas
  val atlasModuleWebApi  = "com.netflix.atlas_v1" %% "atlas-module-webapi" % atlas
  val awsAutoScaling     = "com.amazonaws" % "aws-java-sdk-autoscaling" % aws
  val awsCloudWatch      = "com.amazonaws" % "aws-java-sdk-cloudwatch" % aws
  val awsCore            = "com.amazonaws" % "aws-java-sdk-core" % aws
  val awsDynamoDB        = "com.amazonaws" % "aws-java-sdk-dynamodb" % aws
  val awsEC2             = "com.amazonaws" % "aws-java-sdk-ec2" % aws
  val awsSQS             = "com.amazonaws" % "aws-java-sdk-sqs" % aws
  val awsSTS             = "com.amazonaws" % "aws-java-sdk-sts" % aws
  val aws2SQS            = "software.amazon.awssdk" % "sqs" % aws2
  val frigga             = "com.netflix.frigga" % "frigga" % "0.24.0"
  val guiceCore          = "com.google.inject" % "guice" % guice
  val guiceMulti         = "com.google.inject.extensions" % "guice-multibindings" % guice
  val iepGuice           = "com.netflix.iep" % "iep-guice" % iep
  val iepModuleAdmin     = "com.netflix.iep" % "iep-module-admin" % iep
  val iepModuleArchaius2 = "com.netflix.iep" % "iep-module-archaius2" % iep
  val iepModuleAtlas     = "com.netflix.iep" % "iep-module-atlas" % iep
  val iepModuleAws       = "com.netflix.iep" % "iep-module-aws" % iep
  val iepModuleAws2      = "com.netflix.iep" % "iep-module-aws2" % iep
  val iepModuleAwsMetrics= "com.netflix.iep" % "iep-module-awsmetrics" % iep
  val iepModuleEureka    = "com.netflix.iep" % "iep-module-eureka" % iep
  val iepModuleJmx       = "com.netflix.iep" % "iep-module-jmxport" % iep
  val iepNflxEnv         = "com.netflix.iep" % "iep-nflxenv" % iep
  val iepService         = "com.netflix.iep" % "iep-service" % iep
  val jsonSchema         = "com.github.java-json-tools" % "json-schema-validator" % "2.2.13"
  val jsr305             = "com.google.code.findbugs" % "jsr305" % "3.0.2"
  val log4jApi           = "org.apache.logging.log4j" % "log4j-api" % log4j
  val log4jCore          = "org.apache.logging.log4j" % "log4j-core" % log4j
  val log4jJcl           = "org.apache.logging.log4j" % "log4j-jcl" % log4j
  val log4jJul           = "org.apache.logging.log4j" % "log4j-jul" % log4j
  val log4jSlf4j         = "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4j
  val scalaCompiler      = "org.scala-lang" % "scala-compiler" % scala
  val scalaLibrary       = "org.scala-lang" % "scala-library" % scala
  val scalaLogging       = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  val scalaReflect       = "org.scala-lang" % "scala-reflect" % scala
  val scalatest          = "org.scalatest" %% "scalatest" % "3.1.1"
  val servoCore          = "com.netflix.servo" % "servo-core" % servo
  val slf4jApi           = "org.slf4j" % "slf4j-api" % slf4j
  val slf4jLog4j         = "org.slf4j" % "slf4j-log4j12" % slf4j
  val slf4jSimple        = "org.slf4j" % "slf4j-simple" % slf4j
  val spectatorApi       = "com.netflix.spectator" % "spectator-api" % spectator
  val spectatorAws2      = "com.netflix.spectator" % "spectator-ext-aws2" % spectator
  val spectatorAtlas     = "com.netflix.spectator" % "spectator-reg-atlas" % spectator
  val spectatorLog4j     = "com.netflix.spectator" % "spectator-ext-log4j2" % spectator
  val spectatorM2        = "com.netflix.spectator" % "spectator-reg-metrics2" % spectator
  val spectatorSandbox   = "com.netflix.spectator" % "spectator-ext-sandbox" % spectator
  val typesafeConfig     = "com.typesafe" % "config" % "1.4.0"
}

// format: on

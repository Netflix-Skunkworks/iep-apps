import sbt._

// format: off

object Dependencies {
  object Versions {
    // Do not upgrade Akka, Akka-HTTP, or Alpakka versions, license has changed for newer
    // versions:
    //
    // - https://www.lightbend.com/blog/why-we-are-changing-the-license-for-akka
    // - https://github.com/akka/akka/pull/31561
    // - https://github.com/akka/akka-http/pull/4155
    val akka       = "2.6.21"
    val akkaHttpV  = "10.2.10"

    val atlas      = "1.7.5"
    val aws2       = "2.18.10"
    val iep        = "4.0.2"
    val log4j      = "2.19.0"
    val scala      = "2.13.10"
    val servo      = "0.13.2"
    val slf4j      = "1.7.36"
    val spectator  = "1.3.10"
    val spring     = "5.3.22"
    val avroV      = "1.11.1"

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
  val atlasAkkaTestkit   = "com.netflix.atlas_v1" %% "atlas-akka-testkit" % atlas
  val atlasCore          = "com.netflix.atlas_v1" %% "atlas-core" % atlas
  val atlasEval          = "com.netflix.atlas_v1" %% "atlas-eval" % atlas
  val atlasJson          = "com.netflix.atlas_v1" %% "atlas-json" % atlas
  val atlasSpringAkka    = "com.netflix.atlas_v1" %% "atlas-spring-akka" % atlas
  val atlasSpringEval    = "com.netflix.atlas_v1" %% "atlas-spring-eval" % atlas
  val atlasSpringWebApi  = "com.netflix.atlas_v1" %% "atlas-spring-webapi" % atlas
  val atlasWebApi        = "com.netflix.atlas_v1" %% "atlas-webapi" % atlas
  val aws2AutoScaling    = "software.amazon.awssdk" % "autoscaling" % aws2
  val aws2CloudWatch     = "software.amazon.awssdk" % "cloudwatch" % aws2
  val aws2DynamoDB       = "software.amazon.awssdk" % "dynamodb" % aws2
  val aws2EC2            = "software.amazon.awssdk" % "ec2" % aws2
  val aws2S3             = "software.amazon.awssdk" % "s3" % aws2
  val aws2SQS            = "software.amazon.awssdk" % "sqs" % aws2
  val frigga             = "com.netflix.frigga" % "frigga" % "0.25.0"
  val iepLeaderApi       = "com.netflix.iep" % "iep-leader-api" % iep
  val iepNflxEnv         = "com.netflix.iep" % "iep-nflxenv" % iep
  val iepService         = "com.netflix.iep" % "iep-service" % iep
  val iepSpring          = "com.netflix.iep" % "iep-spring" % iep
  val iepSpringLeaderDDb = "com.netflix.iep" % "iep-spring-leader-dynamodb" % iep
  val iepSpringAdmin     = "com.netflix.iep" % "iep-spring-admin" % iep
  val iepSpringAtlas     = "com.netflix.iep" % "iep-spring-atlas" % iep
  val iepSpringAws2      = "com.netflix.iep" % "iep-spring-aws2" % iep
  val iepSpringJmx       = "com.netflix.iep" % "iep-spring-jmxport" % iep
  val iepSpringLeader    = "com.netflix.iep" % "iep-spring-leader" % iep
  val jsonSchema         = "com.github.java-json-tools" % "json-schema-validator" % "2.2.14"
  val jsr305             = "com.google.code.findbugs" % "jsr305" % "3.0.2"
  val log4jApi           = "org.apache.logging.log4j" % "log4j-api" % log4j
  val log4jCore          = "org.apache.logging.log4j" % "log4j-core" % log4j
  val log4jJcl           = "org.apache.logging.log4j" % "log4j-jcl" % log4j
  val log4jJul           = "org.apache.logging.log4j" % "log4j-jul" % log4j
  val log4jSlf4j         = "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4j
  val munit              = "org.scalameta" %% "munit" % "0.7.29"
  val scalaCompiler      = "org.scala-lang" % "scala-compiler" % scala
  val scalaLibrary       = "org.scala-lang" % "scala-library" % scala
  val scalaLogging       = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
  val scalaReflect       = "org.scala-lang" % "scala-reflect" % scala
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
  val springContext      = "org.springframework" % "spring-context" % spring
  val typesafeConfig     = "com.typesafe" % "config" % "1.4.2"
}

// format: on


lazy val `iep-apps` = project.in(file("."))
  .configure(BuildSettings.profile)
  .aggregate(
    `atlas-aggregator`,
    `atlas-cloudwatch`,
    `atlas-druid`,
    `atlas-persistence`,
    `atlas-slotting`,
    `atlas-stream`,
    `iep-archaius`,
    `iep-atlas`,
    `iep-lwc-bridge`,
    `iep-lwc-cloudwatch-model`,
    `iep-lwc-cloudwatch`,
    `iep-lwc-fwding-admin`,
    `iep-lwc-loadgen`)
  .settings(BuildSettings.noPackaging: _*)

lazy val `atlas-aggregator` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasJson,
    Dependencies.atlasSpringAkka,
    Dependencies.atlasSpringEval,
    Dependencies.caffeine,
    Dependencies.iepDynConfig,
    Dependencies.iepSpring,
    Dependencies.iepSpringAdmin,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.spectatorAtlas,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaTestkit % "test",
    Dependencies.munit % "test"
  ))

lazy val `atlas-cloudwatch` = project
  .configure(BuildSettings.profile)
  .enablePlugins(ProtobufPlugin)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasCore,
    Dependencies.atlasJson,
    Dependencies.atlasSpringAkka,
    Dependencies.aws2CloudWatch,
    Dependencies.frigga,
    Dependencies.iepDynConfig,
    Dependencies.iepSpring,
    Dependencies.iepSpringAws2,
    Dependencies.iepSpringLeader,
    Dependencies.iepSpringLeaderDDb,
    Dependencies.iepService,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.openHFT,
    Dependencies.protobuf,

    Dependencies.atlasWebApi % "test",
    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaTestkit % "test",
    Dependencies.mockitoCore % "test",
    Dependencies.mockitoScala % "test",
    Dependencies.munit % "test"
  ))

lazy val `atlas-druid` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasSpringAkka,
    Dependencies.atlasSpringWebApi,
    Dependencies.iepSpring,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j
  ))

lazy val `atlas-persistence` = project
  .configure(BuildSettings.profile)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.atlasSpringAkka,
      Dependencies.atlasSpringWebApi,
      Dependencies.avro,
      Dependencies.aws2S3,
      Dependencies.iepSpring,
      Dependencies.log4jApi,
      Dependencies.log4jCore,
      Dependencies.log4jSlf4j,
      Dependencies.snappy,
      Dependencies.spectatorAws2
  ))

lazy val `atlas-slotting` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
      Dependencies.akkaHttpCaching,
      Dependencies.atlasSpringAkka,
      Dependencies.aws2AutoScaling,
      Dependencies.aws2DynamoDB,
      Dependencies.aws2EC2,
      Dependencies.iepDynConfig,
      Dependencies.iepSpring,
      Dependencies.iepSpringAdmin,
      Dependencies.iepSpringAws2,
      Dependencies.log4jApi,
      Dependencies.log4jCore,
      Dependencies.log4jSlf4j,
      Dependencies.spectatorAws2,

      Dependencies.akkaHttpTestkit % "test",
      Dependencies.akkaTestkit % "test",
      Dependencies.atlasAkkaTestkit % "test",
      Dependencies.munit % "test"
  ))

lazy val `atlas-stream` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasSpringAkka,
    Dependencies.atlasSpringEval,
    Dependencies.iepSpring,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaTestkit % "test",
    Dependencies.munit % "test"
  ))

lazy val `iep-archaius` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasSpringAkka,
    Dependencies.aws2DynamoDB,
    Dependencies.frigga,
    Dependencies.iepDynConfig,
    Dependencies.iepSpring,
    Dependencies.iepSpringAws2,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.spectatorAws2,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaTestkit % "test",
    Dependencies.atlasAkkaTestkit % "test",
    Dependencies.munit % "test"
  ))

lazy val `iep-atlas` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasSpringAkka,
    Dependencies.atlasSpringWebApi,
    Dependencies.iepSpring,
    Dependencies.iepSpringAdmin,
    Dependencies.iepSpringAtlas,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j
  ))

lazy val `iep-lwc-bridge` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasCore,
    Dependencies.atlasSpringAkka,
    Dependencies.frigga,
    Dependencies.iepSpring,
    Dependencies.iepSpringAtlas,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.spectatorAtlas,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaTestkit % "test",
    Dependencies.atlasAkkaTestkit % "test",
    Dependencies.munit % "test"
  ))

lazy val `iep-lwc-cloudwatch-model` = project
  .configure(BuildSettings.profile)

lazy val `iep-lwc-cloudwatch` = project
  .configure(BuildSettings.profile)
  .dependsOn(`iep-lwc-cloudwatch-model`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasSpringAkka,
    Dependencies.atlasSpringEval,
    Dependencies.aws2CloudWatch,
    Dependencies.iepDynConfig,
    Dependencies.iepSpring,
    Dependencies.iepSpringAtlas,
    Dependencies.iepSpringAws2,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.spectatorAws2,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaTestkit % "test",
    Dependencies.munit % "test"
  ))

lazy val `iep-lwc-fwding-admin` = project
  .configure(BuildSettings.profile)
  .dependsOn(`iep-lwc-cloudwatch-model`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasEval,
    Dependencies.atlasSpringAkka,
    Dependencies.atlasSpringEval,
    Dependencies.aws2DynamoDB,
    Dependencies.iepSpring,
    Dependencies.iepSpringAtlas,
    Dependencies.iepSpringAws2,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.jsonSchema,
    Dependencies.spectatorAws2,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaTestkit % "test",
    Dependencies.atlasAkkaTestkit % "test",
    Dependencies.munit % "test"
  ))


lazy val `iep-lwc-loadgen` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasSpringAkka,
    Dependencies.atlasSpringEval,
    Dependencies.iepSpring,
    Dependencies.iepSpringAtlas,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaTestkit % "test",
    Dependencies.munit % "test"
  ))


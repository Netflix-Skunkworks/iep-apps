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
    Dependencies.atlasSpringPekko,
    Dependencies.atlasSpringEval,
    Dependencies.caffeine,
    Dependencies.iepDynConfig,
    Dependencies.iepSpring,
    Dependencies.iepSpringAdmin,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.spectatorAtlas,

    Dependencies.pekkoHttpTestkit % "test",
    Dependencies.pekkoTestkit % "test",
    Dependencies.munit % "test"
  ))

lazy val `atlas-cloudwatch` = project
  .configure(BuildSettings.profile)
  .enablePlugins(ProtobufPlugin)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasCore,
    Dependencies.atlasJson,
    Dependencies.atlasSpringPekko,
    Dependencies.aws2CloudWatch,
    Dependencies.aws2Config,
    Dependencies.frigga,
    Dependencies.iepDynConfig,
    Dependencies.iepSpring,
    Dependencies.iepSpringAws2,
    Dependencies.iepSpringLeader,
    Dependencies.iepSpringLeaderRds,
    Dependencies.iepService,
    Dependencies.jedis,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.openHFT,
    Dependencies.protobuf,
    Dependencies.spectatorAtlas,
    Dependencies.spectatorApi,

    Dependencies.atlasPekkoTestkit % "test",
    Dependencies.atlasWebApi % "test",
    Dependencies.pekkoHttpTestkit % "test",
    Dependencies.pekkoTestkit % "test",
    Dependencies.mockitoCore % "test",
    Dependencies.mockitoScala % "test",
    Dependencies.munit % "test"
  ))

lazy val `atlas-druid` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasSpringPekko,
    Dependencies.atlasSpringWebApi,
    Dependencies.iepSpring,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
  ))

lazy val `atlas-persistence` = project
  .configure(BuildSettings.profile)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.atlasSpringPekko,
      Dependencies.atlasSpringWebApi,
      Dependencies.avro,
      Dependencies.aws2S3,
      Dependencies.iepSpring,
      Dependencies.iepSpringAws2,
      Dependencies.log4jApi,
      Dependencies.log4jCore,
      Dependencies.log4jSlf4j,
      Dependencies.snappy,
      Dependencies.spectatorAws2
    ))

lazy val `atlas-slotting` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.pekkoHttpCaching,
    Dependencies.atlasSpringPekko,
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

    Dependencies.pekkoHttpTestkit % "test",
    Dependencies.pekkoTestkit % "test",
    Dependencies.atlasPekkoTestkit % "test",
    Dependencies.munit % "test"
  ))

lazy val `atlas-stream` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasSpringPekko,
    Dependencies.atlasSpringEval,
    Dependencies.iepSpring,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,

    Dependencies.pekkoHttpTestkit % "test",
    Dependencies.pekkoTestkit % "test",
    Dependencies.munit % "test"
  ))

lazy val `iep-archaius` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasSpringPekko,
    Dependencies.aws2DynamoDB,
    Dependencies.frigga,
    Dependencies.iepDynConfig,
    Dependencies.iepSpring,
    Dependencies.iepSpringAws2,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.spectatorAws2,

    Dependencies.pekkoHttpTestkit % "test",
    Dependencies.pekkoTestkit % "test",
    Dependencies.atlasPekkoTestkit % "test",
    Dependencies.munit % "test"
  ))

lazy val `iep-lwc-bridge` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasCore,
    Dependencies.atlasSpringPekko,
    Dependencies.frigga,
    Dependencies.iepDynConfig,
    Dependencies.iepSpring,
    Dependencies.iepSpringAtlas,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.spectatorAtlas,

    Dependencies.pekkoHttpTestkit % "test",
    Dependencies.pekkoTestkit % "test",
    Dependencies.atlasPekkoTestkit % "test",
    Dependencies.munit % "test"
  ))

lazy val `iep-lwc-cloudwatch-model` = project
  .configure(BuildSettings.profile)

lazy val `iep-lwc-cloudwatch` = project
  .configure(BuildSettings.profile)
  .dependsOn(`iep-lwc-cloudwatch-model`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasSpringPekko,
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

    Dependencies.pekkoHttpTestkit % "test",
    Dependencies.pekkoTestkit % "test",
    Dependencies.munit % "test"
  ))

lazy val `iep-lwc-fwding-admin` = project
  .configure(BuildSettings.profile)
  .dependsOn(`iep-lwc-cloudwatch-model`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasEval,
    Dependencies.atlasSpringPekko,
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

    Dependencies.pekkoHttpTestkit % "test",
    Dependencies.pekkoTestkit % "test",
    Dependencies.atlasPekkoTestkit % "test",
    Dependencies.munit % "test"
  ))


lazy val `iep-lwc-loadgen` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasSpringPekko,
    Dependencies.atlasSpringEval,
    Dependencies.iepSpring,
    Dependencies.iepSpringAtlas,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,

    Dependencies.pekkoHttpTestkit % "test",
    Dependencies.pekkoTestkit % "test",
    Dependencies.munit % "test"
  ))
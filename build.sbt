
lazy val `iep-apps` = project.in(file("."))
  .configure(BuildSettings.profile)
  .aggregate(
    `atlas-aggregator`,
    `atlas-druid`,
    `atlas-persistence`,
    `atlas-slotting`,
    `atlas-stream`,
    `iep-archaius`,
    `iep-atlas`,
    `iep-clienttest`,
    `iep-lwc-bridge`,
    `iep-lwc-cloudwatch-model`,
    `iep-lwc-cloudwatch`,
    `iep-lwc-fwding-admin`,
    `iep-lwc-loadgen`,
    `iep-ses-monitor`)
  .settings(BuildSettings.noPackaging: _*)

lazy val `atlas-aggregator` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasJson,
    Dependencies.atlasModuleAkka,
    Dependencies.atlasModuleEval,
    Dependencies.iepGuice,
    Dependencies.iepModuleAdmin,
    Dependencies.iepNflxEnv,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.spectatorAtlas,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaTestkit % "test",
    Dependencies.scalatest % "test"
  ))

lazy val `atlas-druid` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasModuleAkka,
    Dependencies.atlasModuleWebApi,
    Dependencies.iepGuice,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j
  ))

lazy val `atlas-persistence` = project
  .configure(BuildSettings.profile)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.atlasModuleAkka,
      Dependencies.atlasModuleWebApi,
      Dependencies.avro,
      Dependencies.aws2S3,
      Dependencies.iepGuice,
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
      Dependencies.atlasModuleAkka,
      Dependencies.aws2AutoScaling,
      Dependencies.aws2DynamoDB,
      Dependencies.aws2EC2,
      Dependencies.iepGuice,
      Dependencies.iepModuleAdmin,
      Dependencies.iepModuleAws2,
      Dependencies.iepNflxEnv,
      Dependencies.log4jApi,
      Dependencies.log4jCore,
      Dependencies.log4jSlf4j,
      Dependencies.spectatorAws2,

      Dependencies.akkaHttpTestkit % "test",
      Dependencies.akkaTestkit % "test",
      Dependencies.scalatest % "test"
  ))

lazy val `atlas-stream` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasModuleAkka,
    Dependencies.atlasModuleEval,
    Dependencies.iepGuice,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaTestkit % "test",
    Dependencies.scalatest % "test"
  ))

lazy val `iep-archaius` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasModuleAkka,
    Dependencies.aws2DynamoDB,
    Dependencies.frigga,
    Dependencies.iepGuice,
    Dependencies.iepModuleAws2,
    Dependencies.iepNflxEnv,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.spectatorAws2,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaTestkit % "test",
    Dependencies.scalatest % "test"
  ))

lazy val `iep-atlas` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasModuleAkka,
    Dependencies.atlasModuleWebApi,
    Dependencies.iepGuice,
    Dependencies.iepModuleAdmin,
    Dependencies.iepModuleArchaius2,
    Dependencies.iepModuleAtlas,
    Dependencies.iepModuleEureka,
    Dependencies.iepModuleJmx,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j
  ))

lazy val `iep-clienttest` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasModuleAkka,
    Dependencies.iepGuice,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.servoCore,
    Dependencies.spectatorApi
  ))

lazy val `iep-lwc-bridge` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasCore,
    Dependencies.atlasModuleAkka,
    Dependencies.frigga,
    Dependencies.iepGuice,
    Dependencies.iepModuleAtlas,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.spectatorAtlas,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaTestkit % "test",
    Dependencies.scalatest % "test"
  ))

lazy val `iep-lwc-cloudwatch-model` = project
  .configure(BuildSettings.profile)

lazy val `iep-lwc-cloudwatch` = project
  .configure(BuildSettings.profile)
  .dependsOn(`iep-lwc-cloudwatch-model`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasModuleAkka,
    Dependencies.atlasModuleEval,
    Dependencies.aws2CloudWatch,
    Dependencies.iepGuice,
    Dependencies.iepModuleAtlas,
    Dependencies.iepModuleAws2,
    Dependencies.iepNflxEnv,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.spectatorAws2,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaTestkit % "test",
    Dependencies.scalatest % "test"
  ))

lazy val `iep-lwc-fwding-admin` = project
  .configure(BuildSettings.profile)
  .dependsOn(`iep-lwc-cloudwatch-model`)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasEval,
    Dependencies.atlasModuleAkka,
    Dependencies.atlasModuleEval,
    Dependencies.aws2DynamoDB,
    Dependencies.iepGuice,
    Dependencies.iepModuleAtlas,
    Dependencies.iepModuleAws2,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.jsonSchema,
    Dependencies.spectatorAws2,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaTestkit % "test",
    Dependencies.scalatest % "test"
  ))


lazy val `iep-lwc-loadgen` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasModuleAkka,
    Dependencies.atlasModuleEval,
    Dependencies.iepGuice,
    Dependencies.iepModuleAtlas,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaTestkit % "test",
    Dependencies.scalatest % "test"
  ))

lazy val `iep-ses-monitor` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasModuleAkka,
    Dependencies.aws2SQS,
    Dependencies.alpakkaSqs,
    Dependencies.iepGuice,
    Dependencies.iepModuleAtlas,
    Dependencies.iepModuleAws2,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.spectatorAws2,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.akkaTestkit % "test",
    Dependencies.scalatest % "test"
  ))


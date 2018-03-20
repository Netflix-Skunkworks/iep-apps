
lazy val root = project.in(file("."))
  .configure(BuildSettings.profile)
  .aggregate(
    `atlas-aggregator`,
    `atlas-druid`,
    `atlas-stream`,
    `iep-archaius`,
    `iep-atlas`,
    `iep-clienttest`,
    `iep-lwc-bridge`,
    `iep-lwc-cloudwatch`)
  .settings(BuildSettings.noPackaging: _*)

lazy val `atlas-aggregator` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasJson,
    Dependencies.atlasModuleAkka,
    Dependencies.atlasModuleEval,
    Dependencies.iepGuice,
    Dependencies.iepNflxEnv,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,
    Dependencies.spectatorAtlas,

    Dependencies.akkaHttpTestkit % "test",
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
    Dependencies.scalatest % "test"
  ))

lazy val `iep-archaius` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasModuleAkka,
    Dependencies.awsDynamoDB,
    Dependencies.awsSTS,
    Dependencies.frigga,
    Dependencies.iepGuice,
    Dependencies.iepNflxEnv,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,

    Dependencies.akkaHttpTestkit % "test",
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
    Dependencies.atlasModuleAkka,
    Dependencies.atlasModuleWebApi,
    Dependencies.frigga,
    Dependencies.iepGuice,
    Dependencies.iepModuleAtlas,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.scalatest % "test"
  ))

lazy val `iep-lwc-cloudwatch` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasModuleAkka,
    Dependencies.atlasModuleEval,
    Dependencies.awsCloudWatch,
    Dependencies.iepGuice,
    Dependencies.iepModuleAtlas,
    Dependencies.iepModuleAws,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.scalatest % "test"
  ))


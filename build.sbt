
lazy val root = project.in(file("."))
  .configure(BuildSettings.profile)
  .aggregate(
    `iep-archaius`,
    `iep-atlas`,
    `iep-lwc-bridge`)
  .settings(BuildSettings.noPackaging: _*)

lazy val `iep-archaius` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasModuleAkka,
    Dependencies.atlasModuleWebApi,
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

lazy val `iep-lwc-bridge` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasModuleAkka,
    Dependencies.atlasModuleWebApi,
    Dependencies.iepGuice,
    Dependencies.iepModuleAtlas,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j,

    Dependencies.akkaHttpTestkit % "test",
    Dependencies.scalatest % "test"
  ))


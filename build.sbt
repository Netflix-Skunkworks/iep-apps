
lazy val root = project.in(file("."))
  .configure(BuildSettings.profile)
  .aggregate(
    `iep-atlas`)
  .settings(BuildSettings.noPackaging: _*)

lazy val `iep-atlas` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasModuleAkka,
    Dependencies.atlasModuleWebApi,
    Dependencies.iepGuice,
    Dependencies.iepModuleAdmin,
    Dependencies.iepModuleArchaius2,
    Dependencies.iepModuleAtlas,
    //Dependencies.iepModuleEureka,
    Dependencies.iepModuleJmx,
    Dependencies.log4jApi,
    Dependencies.log4jCore,
    Dependencies.log4jSlf4j
  ))


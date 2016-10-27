
lazy val root = project.in(file("."))
  .configure(BuildSettings.profile)
  .aggregate(
    `iep-atlas`)
  .settings(BuildSettings.noPackaging: _*)

lazy val `iep-atlas` = project
  .configure(BuildSettings.profile)
  .settings(libraryDependencies ++= Seq(
    Dependencies.atlasMain,
    Dependencies.iepModuleAdmin,
    Dependencies.iepModuleEureka,
    Dependencies.iepModuleJmx
  ))


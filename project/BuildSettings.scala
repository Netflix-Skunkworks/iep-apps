import sbt._
import sbt.Keys._

object BuildSettings {

  val compilerFlags: Seq[String] = Seq(
    "-deprecation",
    "-unchecked",
    "-feature",
    "-release", "21",
  )

  lazy val checkLicenseHeaders = taskKey[Unit]("Check the license headers for all source files.")
  lazy val formatLicenseHeaders = taskKey[Unit]("Fix the license headers for all source files.")

  lazy val baseSettings = GitVersion.settings

  lazy val buildSettings = baseSettings ++ Seq(
    organization := "com.netflix.iep-apps",
    scalaVersion := Dependencies.Versions.scala,
    scalacOptions := {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, _)) => compilerFlags ++ Seq("-Xsource:3", "-Wunused")
        case _            => compilerFlags ++ Seq("-source", "3.3", "-Wunused:all")
      }
    },
    javacOptions ++= Seq("--release", "21"),
    crossPaths := true,
    crossScalaVersions := Dependencies.Versions.crossScala,
    sourcesInBase := false,
    exportJars := true,   // Needed for one-jar, with multi-project
    externalResolvers := BuildSettings.resolvers,

    // Evictions: https://github.com/sbt/sbt/issues/1636
    // Linting: https://github.com/sbt/sbt/pull/5153
    (update / evictionWarningOptions).withRank(KeyRanks.Invisible) := EvictionWarningOptions.empty,

    checkLicenseHeaders := License.checkLicenseHeaders(streams.value.log, sourceDirectory.value),
    formatLicenseHeaders := License.formatLicenseHeaders(streams.value.log, sourceDirectory.value),

    packageBin / packageOptions += Package.ManifestAttributes(
      "Build-Date"   -> java.time.Instant.now().toString,
      "Build-Number" -> sys.env.getOrElse("GITHUB_RUN_ID", "unknown"),
      "Commit"       -> sys.env.getOrElse("GITHUB_SHA", "unknown")
    ),
    testFrameworks += new TestFramework("munit.Framework")
  )

  val commonDeps = Seq(
    Dependencies.jsr305,
    Dependencies.scalaLogging,
    Dependencies.slf4jApi,
    Dependencies.spectatorApi,
    Dependencies.typesafeConfig,
    Dependencies.munit % "test")

  val resolvers = Seq(
    Resolver.mavenLocal,
    Resolver.mavenCentral,
    "NetflixOSS Snapshots" at "https://artifacts-oss.netflix.net/maven-oss-snapshots"
  ) ++ Resolver.sonatypeOssRepos("snapshots")

  // Don't create root.jar, from:
  // http://stackoverflow.com/questions/20747296/producing-no-artifact-for-root-project-with-package-under-multi-project-build-in
  lazy val noPackaging = Seq(
    Keys.`package` :=  file(""),
    packageBin in Global :=  file(""),
    packagedArtifacts :=  Map()
  )

  def profile: Project => Project = p => {
    p.settings(SonatypeSettings.settings)
      .settings(buildSettings: _*)
      .settings(libraryDependencies ++= commonDeps)
  }

  def profileScala2Only: Project => Project = p => {
    p.settings(SonatypeSettings.settings)
      .settings(buildSettings: _*)
      .settings(
        crossScalaVersions := List(Dependencies.Versions.scala),
        skip := {
          scalaVersion.value != Dependencies.Versions.scala
        }
      )
      .settings(libraryDependencies ++= commonDeps)
  }
}

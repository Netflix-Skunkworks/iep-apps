import sbt._
import sbt.Keys._

object BuildSettings {

  val compilerFlags = Seq(
    "-deprecation",
    "-unchecked",
    "-Xlint:_,-infer-any",
    "-feature")

  private val isRecentJdk = System.getProperty("java.specification.version").toDouble >= 11.0

  lazy val checkLicenseHeaders = taskKey[Unit]("Check the license headers for all source files.")
  lazy val formatLicenseHeaders = taskKey[Unit]("Fix the license headers for all source files.")

  lazy val storeBintrayCredentials = taskKey[Unit]("Store bintray credentials.")
  lazy val credentialsFile = Path.userHome / ".bintray" / ".credentials"

  lazy val baseSettings = GitVersion.settings

  lazy val buildSettings = baseSettings ++ Seq(
    organization := "com.netflix.iep-apps",
    scalaVersion := Dependencies.Versions.scala,
    scalacOptions ++= {
      // -release option is not supported in scala 2.11
      val v = scalaVersion.value
      CrossVersion.partialVersion(v).map(_._2.toInt) match {
        case Some(v) if v > 11 && isRecentJdk => compilerFlags ++ Seq("-release", "8")
        case _                                => compilerFlags ++ Seq("-target:jvm-1.8")
      }
    },
    javacOptions ++= {
      if (isRecentJdk)
        Seq("--release", "8")
      else
        Seq("-source", "1.8", "-target", "1.8")
    },
    crossPaths := true,
    crossScalaVersions := Dependencies.Versions.crossScala,
    sourcesInBase := false,
    exportJars := true,   // Needed for one-jar, with multi-project
    externalResolvers := BuildSettings.resolvers,

    // Evictions: https://github.com/sbt/sbt/issues/1636
    // Linting: https://github.com/sbt/sbt/pull/5153
    (evictionWarningOptions in update).withRank(KeyRanks.Invisible) := EvictionWarningOptions.empty,

    checkLicenseHeaders := License.checkLicenseHeaders(streams.value.log, sourceDirectory.value),
    formatLicenseHeaders := License.formatLicenseHeaders(streams.value.log, sourceDirectory.value),

    storeBintrayCredentials := {
      IO.write(
        credentialsFile,
        bintray.BintrayCredentials.api.template(Bintray.user, Bintray.pass))
    },

    packageOptions in (Compile, packageBin) += Package.ManifestAttributes(
      "Build-Date"   -> java.time.Instant.now().toString,
      "Build-Number" -> sys.env.getOrElse("GITHUB_RUN_ID", "unknown"),
      "Commit"       -> sys.env.getOrElse("GITHUB_SHA", "unknown")
    )
  )

  val commonDeps = Seq(
    Dependencies.jsr305,
    Dependencies.scalaLogging,
    Dependencies.slf4jApi,
    Dependencies.spectatorApi,
    Dependencies.typesafeConfig,
    Dependencies.scalatest % "test")

  val resolvers = Seq(
    Resolver.mavenLocal,
    Resolver.mavenCentral,
    Resolver.jcenterRepo,
    "jfrog" at "https://oss.jfrog.org/oss-snapshot-local"
  )

  // Don't create root.jar, from:
  // http://stackoverflow.com/questions/20747296/producing-no-artifact-for-root-project-with-package-under-multi-project-build-in
  lazy val noPackaging = Seq(
    Keys.`package` :=  file(""),
    packageBin in Global :=  file(""),
    packagedArtifacts :=  Map()
  )

  def profile: Project => Project = p => {
    bintrayProfile(p)
      .settings(buildSettings: _*)
      .settings(libraryDependencies ++= commonDeps)
  }

  // Disable bintray plugin when not running under CI. Avoids a bunch of warnings like:
  //
  // ```
  // Missing bintray credentials /Users/brharrington/.bintray/.credentials. Some bintray features depend on this.
  // [warn] Credentials file /Users/brharrington/.bintray/.credentials does not exist
  // ```
  def bintrayProfile(p: Project): Project = {
    if (credentialsFile.exists)
      p.settings(Bintray.settings)
    else
      p.disablePlugins(bintray.BintrayPlugin)
  }
}

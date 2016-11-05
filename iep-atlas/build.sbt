
enablePlugins(JavaServerAppPackaging)

maintainer := "Brian Harrington"
packageSummary := "Atlas packaging example."
packageDescription := """Example for how to create a package based on
  the Atlas jars."""

mainClass in Compile := Some("com.netflix.iep.guice.Main")

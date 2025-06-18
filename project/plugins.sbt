addSbtPlugin("com.github.sbt"            % "sbt-pgp"              % "2.3.1")
addSbtPlugin("com.github.sbt"            % "sbt-release"          % "1.4.0")
addSbtPlugin("pl.project13.scala"        % "sbt-jmh"              % "0.4.7")
addSbtPlugin("com.github.sbt"            % "sbt-git"              % "2.0.1")
addSbtPlugin("com.typesafe.sbt"          % "sbt-native-packager"  % "1.3.12")

addSbtPlugin("org.scalameta"             % "sbt-scalafmt"         % "2.4.6")

// for compiling protobuf in the Cloud Watch module
addSbtPlugin("com.github.sbt"            % "sbt-protobuf"         % "0.8.0")

// Convenient helpers, not required
addSbtPlugin("com.timushev.sbt"          % "sbt-updates"          % "0.6.4")

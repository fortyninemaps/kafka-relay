ThisBuild / scalaVersion := "2.13.1"

libraryDependencies += "org.typelevel" %% "cats-core" % "2.1.1"
libraryDependencies += "org.typelevel" %% "cats-effect" % "2.1.2"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.4.1"
libraryDependencies += "com.monovore" %% "decline" % "1.0.0"

// scalacOptions += "-Ypartial-unification"

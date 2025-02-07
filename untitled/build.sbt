ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.5"

lazy val root = (project in file("."))
  .settings(
    name := "untitled",
    idePackagePrefix := Some("ec.edu.utpl.prencencial.computacion.pfr.integrador"),
    libraryDependencies ++= Seq(
      "com.github.tototoshi" %% "scala-csv" % "2.0.0",
      "org.tpolecat"         %% "doobie-hikari" % "1.0.0-RC5",
      "org.tpolecat"         %% "doobie-hikari" % "1.0.0-RC5",
      "com.mysql"             % "mysql-connector-j" % "9.1.0"
    )
  )

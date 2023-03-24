ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.8"

lazy val root = (project in file("."))
  .settings(
    name := "spark-dst-issue-scala-unit-test"
  )
libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.3.0"
//libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.1"
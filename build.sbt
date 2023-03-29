ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.8"

lazy val root = (project in file("."))
  .settings(
    name := "spark-dst-issue-scala-unit-test"
  )
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"
libraryDependencies+="org.scalatest" %% "scalatest" % "3.2.15" % Test
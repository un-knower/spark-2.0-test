import sbt.Keys._

name := "scala-base"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.42"
libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.10.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.10.0"
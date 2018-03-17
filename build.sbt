import sbt._
import sbt.Keys._
import Dependencies._

name := "spark-2.0-test"

version := "1.0"

scalaVersion := "2.11.8"

lazy val commonSettings = Seq(
  scalaVersion := "2.11.8"
)

externalResolvers += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/snapshots"
externalResolvers += "Typesafe Simple Repository" at
  "http://repo.typesafe.com/typesafe/simple/maven-releases/"

updateOptions := updateOptions.value.withCachedResolution(true)


lazy val kafka_010 = (project in file("kafka_010"))
  .settings(commonSettings: _*)

lazy val test_lib = Seq(
    "org.scalactic" %% "scalactic" % "3.0.1",
    "org.scalatest" %% "scalatest" % "3.0.1"
)

lazy val spark_base = (project in file("spark_base"))
.settings(commonSettings: _*)
.settings(libraryDependencies ++= test_lib)


lazy val structured_streaming = (project in file("structured_streaming"))
.settings(commonSettings: _*)
.settings(libraryDependencies ++= test_lib)

lazy val structured_streaming_runner = project.in(file("structured_streaming_runner")).dependsOn(structured_streaming).settings(
    scalaVersion := "2.11.8",
    libraryDependencies ++= spark_lib.map(_ % "compile"),
    name := "structured_streaming_runner"
).disablePlugins(sbtassembly.AssemblyPlugin)


lazy val spark_streaming = (project in file("spark_streaming"))
.settings(commonSettings: _*)
.settings(libraryDependencies ++= test_lib)


lazy val spark_source = (project in file("spark_source"))
.settings(commonSettings: _*)
.settings(libraryDependencies ++= test_lib)


lazy val spark_ml = (project in file("spark_ml"))
.settings(commonSettings:_*)
.settings(libraryDependencies ++= test_lib)


lazy val scala_base = (project in file("scala_base"))
.settings(commonSettings:_*)
.settings(libraryDependencies ++= test_lib)
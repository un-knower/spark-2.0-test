import sbt._
import Process._
import sbt.Keys._

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

externalResolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

lazy val spark_version = "2.1.0"
lazy val spark_lib = Seq(
  "org.apache.spark" %% "spark-core" % spark_version,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % spark_version,
  "org.apache.spark" %% "spark-sql" % spark_version,
  "org.apache.spark" %% "spark-streaming" % spark_version,
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % spark_version
)

lazy val kafka_010 = (project in file("kafka_010"))
  .settings(commonSettings: _*)


lazy val spark_base = (project in file("spark_base")).settings(commonSettings: _*).settings(
  libraryDependencies ++= spark_lib
)

lazy val structured_streaming = (project in file("structured_streaming"))
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= spark_lib
  )

lazy val spark_streaming = (project in file("spark_streaming"))
  .settings(commonSettings: _*)

lazy val spark_source = (project in file("spark_source"))

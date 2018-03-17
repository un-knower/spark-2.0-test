import sbt.Keys._

name := "spark-ml"

version := "1.0"

scalaVersion := "2.11.8"

lazy val spark_version = "2.3.0"
lazy val spark_lib = Seq(
    "org.apache.spark" %% "spark-core" % spark_version,
    "org.apache.spark" %% "spark-sql" % spark_version,
    "org.apache.spark" %% "spark-streaming" % spark_version,
    "org.apache.spark" %% "spark-mllib" % spark_version
)

libraryDependencies ++= spark_lib
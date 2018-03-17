import sbt.Keys._

name := "spark-streaming"

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

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % spark_version

libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.10.1.0"

libraryDependencies += "com.typesafe" % "config" % "1.2.1"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.42"

libraryDependencies += "redis.clients" % "jedis" % "2.9.0"


lazy val hbase_lib = Seq(
    "org.apache.hbase" % "hbase-client" % "1.2.4" % "provided",
    "org.apache.hbase" % "hbase-common" % "1.2.4" % "provided",
    "org.apache.hbase" % "hbase-server" % "1.2.4"  % "provided" excludeAll ExclusionRule(organization = "org.mortbay.jetty")
)
libraryDependencies ++= hbase_lib

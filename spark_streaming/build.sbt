import sbt.Keys._

name := "spark_streaming"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.10.1.0"

libraryDependencies += "com.typesafe" % "config" % "1.2.1"

libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.6"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies += "redis.clients" % "jedis" % "2.9.0"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.4"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.4"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.4" excludeAll ExclusionRule(organization = "org.mortbay.jetty")

lazy val spark_version = "2.1.0"
lazy val spark_lib = Seq(
  "org.apache.spark" %% "spark-core" % spark_version % "compile",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % spark_version % "compile",
  "org.apache.spark" %% "spark-sql" % spark_version % "compile",
  "org.apache.spark" %% "spark-streaming" % spark_version % "compile",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % spark_version % "compile"
)

libraryDependencies ++= spark_lib

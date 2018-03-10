import sbt.Keys._

name := "spark-streaming"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.10.1.0"

libraryDependencies += "com.typesafe" % "config" % "1.2.1"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.42"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "provided"

libraryDependencies += "redis.clients" % "jedis" % "2.9.0"


lazy val hbase_lib = Seq(
    "org.apache.hbase" % "hbase-client" % "1.2.4" % "provided",
    "org.apache.hbase" % "hbase-common" % "1.2.4" % "provided",
    "org.apache.hbase" % "hbase-server" % "1.2.4"  % "provided" excludeAll ExclusionRule(organization = "org.mortbay.jetty")
)
libraryDependencies ++= hbase_lib

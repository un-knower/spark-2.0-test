name := "spark-base"

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

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.38"

libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.0"

libraryDependencies += "org.jblas" % "jblas" % "1.2.4"

// libraryDependencies += "org.apache.hbase" % "hbase-spark" % "2.0.0-alpha4"

unmanagedBase <<= baseDirectory { base => base / "lib" }


val hbase_lib = Seq(
    "org.apache.hbase" % "hbase-client" % "1.2.5" ,
    "org.apache.hbase" % "hbase-common" % "1.2.5" ,
    "org.apache.hbase" % "hbase-server" % "1.2.5" excludeAll ExclusionRule(organization = "org.mortbay.jetty")
)
libraryDependencies ++= hbase_lib

libraryDependencies += "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.5"

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
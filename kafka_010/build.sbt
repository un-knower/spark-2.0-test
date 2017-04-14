name := "kafka_010"

scalaVersion := "2.11.8"

version := "1.0"

// resolvers += "jfrog" at "https://dl.bintray.com/cakesolutions/maven"

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.1.0"

libraryDependencies += "net.cakesolutions" %% "scala-kafka-client" % "0.10.1.1"

libraryDependencies += "net.cakesolutions" %% "scala-kafka-client-akka" % "0.10.1.1"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.4.14"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

libraryDependencies ++= Seq(
    "com.typesafe.slick" %% "slick" % "3.1.1",
    "org.slf4j" % "slf4j-nop" % "1.6.4"
)

libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.3"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

logBuffered in Test := false

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")
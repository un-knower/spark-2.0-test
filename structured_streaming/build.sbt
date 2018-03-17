name := "structured-streaming"

scalaVersion := "2.11.8"

lazy val spark_version = "2.3.0"
lazy val spark_lib = Seq(
    "org.apache.spark" %% "spark-core" % spark_version,
    "org.apache.spark" %% "spark-sql" % spark_version,
    "org.apache.spark" %% "spark-streaming" % spark_version,
    "org.apache.spark" %% "spark-mllib" % spark_version
)


libraryDependencies ++= spark_lib.map(_ % "provided")

libraryDependencies += "org.apache.curator" % "curator-framework" % "3.3.0"
libraryDependencies +=  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % spark_version
libraryDependencies +=  "joda-time" % "joda-time" % "2.9.9"

run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated

test in assembly := {}
assemblyJarName in assembly := "structured-streaming.jar"
assemblyOption in assembly := (assemblyOption in assembly).value.copy(cacheUnzip = false)
assemblyExcludedJars in assembly := {
    val cp = (fullClasspath in assembly).value
    cp filter { el =>
        (el.data.getName == "unused-1.0.0.jar") ||
        (el.data.getName == "spark-tags_"+scalaVersion+"-"+spark_version+".jar")
    }
}

name := "SparkLearning"

version := "0.1"

scalaVersion := "2.11.8"

val scalaTestVersion = "2.2.4"

libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.0"

parallelExecution in Test := false
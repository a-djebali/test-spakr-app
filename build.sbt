name := "ChallengeApp"

version := "0.1"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

val sparkVersion = "2.0.2"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "org.apache.hadoop" % "hadoop-common" % "2.7.3"
)

libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.4"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % "test"
libraryDependencies += "io.netty" % "netty-all" % "4.0.23.Final"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.0.2_0.7.4" % "test"

parallelExecution in Test := false
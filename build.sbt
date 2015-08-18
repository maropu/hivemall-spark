name := "hivemall-spark"

version := "0.0.4"

scalaVersion := "2.10.4"

// Since spark-v1.4.0, it only bundles Hive jars of version 0.13.1.
// https://spark.apache.org/docs/latest/sql-programming-guide.html#interacting-with-different-versions-of-hive-metastore
unmanagedSourceDirectories in Compile += baseDirectory.value / "extra-src/v0.13.1"

// To avoid compiler errors in sbt-doc
// sources in doc in Compile := List()

// To skip unit tests in assembly
test in assembly := {}

net.virtualvoid.sbt.graph.Plugin.graphSettings

// spark-package settings
spName := "maropu/hivemall-spark"

sparkVersion := "1.4.1"

sparkComponents ++= Seq("sql", "mllib", "hive")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

// resolvers += Resolver.sonatypeRepo("releases")
// addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0-M5" cross CrossVersion.full)

libraryDependencies ++= Seq(
  "io.github.myui" % "hivemall" % "0.3.1",
  "org.scalatest" % "scalatest_2.10" % "2.2.4" % "provided",
  "org.xerial" % "xerial-core" % "3.2.3" % "provided"
)

mergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".properties" =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".xml" =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".types" =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".class" =>
    MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".thrift" =>
    MergeStrategy.first
  case "application.conf" =>
    MergeStrategy.concat
  case "unwanted.txt" =>
    MergeStrategy.discard
  case x =>
    val oldStrategy = (mergeStrategy in assembly).value
    oldStrategy(x)
}


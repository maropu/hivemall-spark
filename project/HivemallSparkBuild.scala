import sbt._
import sbt.Keys._

object HivemallSparkBuild extends Build {

  lazy val hivemallspark = Project(
    id = "hivemall-spark",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "hivemall-spark",
      organization := "hivemall",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.10.4"
      // add other settings here
    )
  )
}

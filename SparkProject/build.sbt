import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.10.4",
      version      := "1.0"
    )),
    name := "SparkProject",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"
  )

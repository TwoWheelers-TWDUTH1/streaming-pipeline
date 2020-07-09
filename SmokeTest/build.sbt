val sparkVersion = "2.3.0"

lazy val root = (project in file(".")).

  settings(
    inThisBuild(List(
      organization := "com.tw",
      scalaVersion := "2.11.8",
      version := "0.0.1"
    )),

    name := "tw-data-smoke-test",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.amazonaws" % "aws-java-sdk" % "1.11.818",

      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "org.mockito" % "mockito-core" % "3.3.3" % "test"
    )
  )

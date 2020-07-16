val sparkVersion = "2.3.0"
// Exclude mysterious dependency, otherwise things start failing for some tests.
// https://stackoverflow.com/questions/50907437/sbt-test-error-java-lang-nosuchmethoderror-net-jpountz-lz4-lz4blockinputstream
lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
lazy val root = (project in file(".")).

  settings(
    inThisBuild(List(
      organization := "com.tw",
      scalaVersion := "2.11.8",
      version := "0.0.1",

      testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-v", "--ignore-runners=org.specs2.runner.JUnitRunner")
    )),

    name := "tw-station-consumer",

    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.1.0" % "test",
      "org.scalamock" %% "scalamock" % "4.4.0" % "test",
      "org.apache.kafka" %% "kafka" % "0.10.0.1" % "test" excludeAll(excludeJpountz),
      "org.apache.curator" % "curator-test" % "2.10.0" % "test",
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion excludeAll(excludeJpountz),
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion excludeAll(excludeJpountz),
      "com.amazonaws" % "aws-java-sdk" % "1.11.818"
    )
  )

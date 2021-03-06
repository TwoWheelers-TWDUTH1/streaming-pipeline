package com.tw.apps

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

class StationAggregationTest extends AnyFeatureSpec with Matchers with GivenWhenThen {
  val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()

  import spark.implicits._

  Feature("Select most updated data of each station") {

    Scenario("Multiple data on some station") {

      Given("Sample station data")
      val columns = Seq("bikes_available", "docks_available", "is_renting", "is_returning", "last_updated", "station_id", "name", "latitude", "longitude")
      val testStationData = Seq(
        (0, 0, false, false, 1594882600, "0deb7e762d80f771360306ef132bce3d", "Valencia St at 16th St", 37.765052, -122.4218661),
        (11, 12, true, true, 1594882601, "0deb7e762d80f771360306ef132bce3d", "Valencia St at 16th St", 37.765052, -122.4218661)
      )
      val testStationDS = testStationData.toDF(columns:_*).as[StationData]

      When("Most updated data is selected")
      val outputDS = StationAggregation.selectMostUpdatedStationData(testStationDS, spark)

      Then("Only most updated data remained")
      val expectedStationData = Array(
        StationData(11, 12, true, true, 1594882601, "0deb7e762d80f771360306ef132bce3d", "Valencia St at 16th St", 37.765052, -122.4218661)
      )
      outputDS.collect shouldBe expectedStationData
    }

    Scenario("Write aggregated station data to data sink") {
      Given("Sample station data")
      val inputDataPath = getClass.getResource("/data").toURI
      val schema = ScalaReflection.schemaFor[StationData].dataType.asInstanceOf[StructType]
      val testStationDS = spark.readStream
          .format("csv")
          .schema(schema)
          .option("header", true)
          .option("path", inputDataPath.toString)
          .option("includeTimestamp", true)
          .load()
          .as[StationData]

      When("Most updated data is selected")
      val outputDS = StationAggregation.selectMostUpdatedStationData(testStationDS, spark)

      Then("Only most updated data remained")
      val outputQuery = outputDS.writeStream
        .format("memory")
        .queryName("Output")
        .outputMode("update")
        .start()
      outputQuery.processAllAvailable()
      val results = spark.sql("select * from Output").collect
      val expectedStationData = Array(
        Row(11, 12, true, true, 1594882601, "0deb7e762d80f771360306ef132bce3d", "Valencia St at 16th St", 37.765052, -122.4218661),
        Row(11, 12, true, true, 1604882800, "0deb7e762d80f771360306ef132bce3d", "Valencia St at 16th St", 37.765052, -122.4218661)
      )
      results shouldBe (expectedStationData)
    }

  }
}
package com.tw.apps

import com.tw.apps.StationDataTransformation.{nycStationStatusJson2DF, citybikeV2StationStatusJson2DF}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers

import scala.io.Source


class StationDataTransformationTest extends AnyFeatureSpec with Matchers with GivenWhenThen {
  val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()
  import spark.implicits._

  private val BIKES_AVAILABLE_INDEX = 0
  private val DOCKS_AVAILABLE_INDEX = 1
  private val IS_RENTING_INDEX = 2
  private val IS_RETURNING_INDEX = 3
  private val LAST_UPDATED_INDEX = 4
  private val STATION_ID_INDEX = 5
  private val NAME_INDEX = 6
  private val LATITUDE_INDEX = 7
  private val LONGITUDE_INDEX = 8
  Feature("Apply station status transformations to data frame") {

    Scenario("Transform nyc station data frame") {

      val testStationData =
        """{
          "station_id":"83",
          "bikes_available":19,
          "docks_available":41,
          "is_renting":true,
          "is_returning":true,
          "last_updated":1536242527,
          "name":"Atlantic Ave & Fort Greene Pl",
          "latitude":40.68382604,
          "longitude":-73.97632328
          }"""

      val schema = ScalaReflection.schemaFor[StationData].dataType //.asInstanceOf[StructType]

      Given("Sample data for station_status")
      val testDF1 = Seq(testStationData).toDF("raw_payload")


      When("Transformations are applied")
      val resultDF1 = testDF1.transform(nycStationStatusJson2DF(_, spark))

      Then("Useful columns are extracted")
      validateStationInformationSchema(resultDF1)

      val row1 = resultDF1.head()
      row1.get(BIKES_AVAILABLE_INDEX) should be(19)
      row1.get(DOCKS_AVAILABLE_INDEX) should be(41)
      row1.get(IS_RENTING_INDEX) shouldBe true
      row1.get(IS_RETURNING_INDEX) shouldBe true
      row1.get(LAST_UPDATED_INDEX) should be(1536242527)
      row1.get(STATION_ID_INDEX) should be("83")
      row1.get(NAME_INDEX) should be("Atlantic Ave & Fort Greene Pl")
      row1.get(LATITUDE_INDEX) should be(40.68382604)
      row1.get(LONGITUDE_INDEX) should be(-73.97632328)
    }

    Scenario("Transform SF station data frame") {

      val testStationData = Source.fromFile(getClass.getResource("/sf-example-payload.json").toURI).mkString

      Given("Sample data for station_status")
      val testDF1 = Seq(testStationData).toDF("raw_payload")


      When("Transformations are applied")
      val resultDF1 = testDF1.transform(citybikeV2StationStatusJson2DF(_, spark))

      Then("Useful columns are extracted")
      validateStationInformationSchema(resultDF1)

      val row1 = resultDF1.head()
      row1.get(BIKES_AVAILABLE_INDEX) should be(7)
      row1.get(DOCKS_AVAILABLE_INDEX) should be(8)
      row1.get(IS_RENTING_INDEX) shouldBe true
      row1.get(IS_RETURNING_INDEX) shouldBe true
      row1.get(LAST_UPDATED_INDEX) should be(1594887440)
      row1.get(STATION_ID_INDEX) should be("d0e8f4f1834b7b33a3faf8882f567ab8")
      row1.get(NAME_INDEX) should be("Harmon St at Adeline St")
      row1.get(LATITUDE_INDEX) should be(37.849735)
      row1.get(LONGITUDE_INDEX) should be(-122.270582)
    }

    Scenario("Transform Marseille station data frame") {

      val testStationData = Source.fromFile(getClass.getResource("/marseille-example-payload.json").toURI).mkString

      Given("Sample data for station_status")
      val testDF1 = Seq(testStationData).toDF("raw_payload")


      When("Transformations are applied")
      val resultDF1 = testDF1.transform(citybikeV2StationStatusJson2DF(_, spark))

      Then("Useful columns are extracted")
      validateStationInformationSchema(resultDF1)

      val row1 = resultDF1.head()
      row1.get(BIKES_AVAILABLE_INDEX) should be(8)
      row1.get(DOCKS_AVAILABLE_INDEX) should be(11)
      row1.get(IS_RENTING_INDEX) shouldBe true
      row1.get(IS_RETURNING_INDEX) shouldBe true
      row1.get(LAST_UPDATED_INDEX) should be(1594891680)
      row1.get(STATION_ID_INDEX) should be("686e48654a218c70daf950a4e893e5b0")
      row1.get(NAME_INDEX) should be("8149-391 MICHELET")
      row1.get(LATITUDE_INDEX) should be(43.25402727813068)
      row1.get(LONGITUDE_INDEX) should be(5.401873594694653)
    }
  }

  private def validateStationInformationSchema(resultDF1: Dataset[Row]) = {
        resultDF1.schema.fields(BIKES_AVAILABLE_INDEX).name should be("bikes_available")
        resultDF1.schema.fields(BIKES_AVAILABLE_INDEX).dataType.typeName should be("integer")
        resultDF1.schema.fields(DOCKS_AVAILABLE_INDEX).name should be("docks_available")
        resultDF1.schema.fields(DOCKS_AVAILABLE_INDEX).dataType.typeName should be("integer")
        resultDF1.schema.fields(IS_RENTING_INDEX).name should be("is_renting")
        resultDF1.schema.fields(IS_RENTING_INDEX).dataType.typeName should be("boolean")
        resultDF1.schema.fields(IS_RETURNING_INDEX).name should be("is_returning")
        resultDF1.schema.fields(IS_RETURNING_INDEX).dataType.typeName should be("boolean")
        resultDF1.schema.fields(LAST_UPDATED_INDEX).name should be("last_updated")
        resultDF1.schema.fields(LAST_UPDATED_INDEX).dataType.typeName should be("long")
        resultDF1.schema.fields(STATION_ID_INDEX).name should be("station_id")
        resultDF1.schema.fields(STATION_ID_INDEX).dataType.typeName should be("string")
        resultDF1.schema.fields(NAME_INDEX).name should be("name")
        resultDF1.schema.fields(NAME_INDEX).dataType.typeName should be("string")
        resultDF1.schema.fields(LATITUDE_INDEX).name should be("latitude")
        resultDF1.schema.fields(LATITUDE_INDEX).dataType.typeName should be("double")
        resultDF1.schema.fields(LONGITUDE_INDEX).name should be("longitude")
        resultDF1.schema.fields(LONGITUDE_INDEX).dataType.typeName should be("double")
  }
}

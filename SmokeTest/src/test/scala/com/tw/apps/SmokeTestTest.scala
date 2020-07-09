package com.tw.apps

import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import com.amazonaws.services.cloudwatch.model.{PutMetricDataRequest, StandardUnit}
import org.apache.spark.sql.SparkSession
import org.mockito.{ArgumentCaptor, Mockito}
import org.mockito.Mockito.verify
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

class SmokeTestTest extends FeatureSpec with MockitoSugar with Matchers with GivenWhenThen with BeforeAndAfter {
  feature("Apply station status transformations to data frame") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()
    val cloudWatchMock: AmazonCloudWatch = mock[AmazonCloudWatch]

    before {
      Mockito.reset(cloudWatchMock)
    }

    scenario("passes and returns the latest `last_updated`") {
      val validDF = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv("./src/test/resources/valid.csv")
        .cache()

      SmokeTest.runAssertions(validDF, cloudWatchMock, 1594115095)
    }

    scenario("explodes when there are null longitudes") {
      val validDF = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv("./src/test/resources/has-null-longitude.csv")
        .cache()

      val exception = intercept[AssertionError] {
        SmokeTest.runAssertions(validDF, cloudWatchMock, 1594115095)
      }
      assert(exception.getMessage.contains("longitude has null values"))
    }

    scenario("explodes when there are null latitudes") {
      val validDF = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv("./src/test/resources/has-null-latitude.csv")
        .cache()

      val exception = intercept[AssertionError] {
        SmokeTest.runAssertions(validDF, cloudWatchMock, 1594115095)
      }

      assert(exception.getMessage.contains("latitude has null values"))
    }

    scenario("explodes when there are duplicated station_ids") {
      val validDF = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv("./src/test/resources/has-duplicated-station-ids.csv")
        .cache()

      val exception = intercept[AssertionError] {
        SmokeTest.runAssertions(validDF, cloudWatchMock, 1594115095)
      }

      assert(exception.getMessage.contains("there are duplicate stations"))
    }

    scenario("explodes when there are negative counts") {
      val validDF = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv("./src/test/resources/has-negative-counts.csv")
        .cache()

      val exception = intercept[AssertionError] {
        SmokeTest.runAssertions(validDF, cloudWatchMock, 1594115095)
      }

      assert(exception.getMessage.contains("negative counts"))
    }

    scenario("reports the median age of station updates") {
      val validDF = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv("./src/test/resources/valid.csv")
        .cache()

      SmokeTest.runAssertions(validDF, cloudWatchMock, 1594115095)
      val requestCaptor : ArgumentCaptor[PutMetricDataRequest] = ArgumentCaptor.forClass(classOf[PutMetricDataRequest])

      verify(cloudWatchMock).putMetricData(requestCaptor.capture())
      val request = requestCaptor.getValue
      val metricDatum = request.getMetricData.get(0)

      metricDatum.getValue shouldEqual(10.0)
      metricDatum.getUnit shouldEqual(StandardUnit.Seconds.toString)
      request.getNamespace shouldEqual("stationMart-monitoring")
    }
  }
}

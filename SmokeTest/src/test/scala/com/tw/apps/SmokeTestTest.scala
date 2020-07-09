package com.tw.apps

import org.apache.spark.sql.SparkSession
import org.scalatest._

class SmokeTestTest extends FeatureSpec with Matchers with GivenWhenThen {

  feature("Apply station status transformations to data frame") {
    val spark = SparkSession.builder.appName("Test App").master("local").getOrCreate()

    scenario("passes and returns the latest `last_updated`") {
      val validDF = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv("./src/test/resources/valid.csv")
        .cache()

      SmokeTest.runAssertions(validDF)
    }

    scenario("explodes when there are null longitudes") {
      val validDF = spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .csv("./src/test/resources/has-null-longitude.csv")
        .cache()

      val exception = intercept[AssertionError] {
        SmokeTest.runAssertions(validDF)
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
        SmokeTest.runAssertions(validDF)
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
        SmokeTest.runAssertions(validDF)
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
        SmokeTest.runAssertions(validDF)
      }

      assert(exception.getMessage.contains("negative counts"))
    }
  }
}

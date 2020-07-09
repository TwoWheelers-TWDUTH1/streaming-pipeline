package com.tw.apps

import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}
import com.amazonaws.services.cloudwatch.model.{MetricDatum, PutMetricDataRequest, StandardUnit}
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SmokeTest {

  def assertThatLatitudeAndLongitudeAreNeverNull(df: sql.DataFrame) = {
    val nullLongitudeCount = df.where("longitude is null").count()
    assert(nullLongitudeCount == 0, "longitude has null values")

    val nullLatitudeCount = df.where("latitude is null").count()
    assert(nullLatitudeCount == 0, "latitude has null values")

    true
  }

  def assertThatNoCountsAreNegative(df: sql.DataFrame): Unit = {
    val stationsWithNegativeCount = df.
        where("bikes_available < 0 OR docks_available < 0")

    assert(stationsWithNegativeCount.count() == 0, "There are stations with negative counts (!)")

    true
  }

  def assertThatUpdatesAreRecent(df: DataFrame, cw: AmazonCloudWatch, currentTimeInSeconds: Long): Unit = {
    val medianLastUpdateUnixTimestamp = df.stat.approxQuantile("last_updated", Array(0.5), 0.25)(0)
    val currentUnixTimestamp = currentTimeInSeconds

    val diffInSeconds = currentUnixTimestamp - medianLastUpdateUnixTimestamp

    val datum = new MetricDatum()
      .withMetricName("station-last-updated-age")
      .withUnit(StandardUnit.Seconds)
      .withValue(diffInSeconds)

    val request = new PutMetricDataRequest()
      .withNamespace("stationMart-monitoring")
      .withMetricData(datum)

    cw.putMetricData(request)

    assert(diffInSeconds < 10 * 60, s"Median station update age is ${diffInSeconds}s but was supposed to be less than 600s")
  }


  def runAssertions(df: DataFrame, cw: AmazonCloudWatch, currentTimeInSeconds: Long) = {
    assertThatStationIdsAreUnique(df)
    assertThatLatitudeAndLongitudeAreCorrectlyTyped(df)
    assertThatLatitudeAndLongitudeAreNeverNull(df)
    assertThatNoCountsAreNegative(df)
    assertThatUpdatesAreRecent(df, cw, currentTimeInSeconds)
    true
  }

  def assertThatLatitudeAndLongitudeAreCorrectlyTyped(df: sql.DataFrame) = {
    assert(df.schema.fields(7).name == "latitude", "field isn't called `latitude`")
    assert(df.schema.fields(7).dataType.typeName == "double", "field `latitude` is not a double")
    assert(df.schema.fields(8).name == "longitude", "field isn't called `longitude`")
    assert(df.schema.fields(8).dataType.typeName == "double", "field `longitude` is not a double")

    true
  }

  def assertThatStationIdsAreUnique(df: sql.DataFrame) = {
    val stationsIdsWithMoreThanOneRow = df.select("station_id").groupBy("station_id").agg(count("station_id") as "count").where("count > 1")
    assert(stationsIdsWithMoreThanOneRow.count() == 0, "there are duplicate stations")
    true
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      val message = "Argument required: <file>"
      throw new IllegalArgumentException(message)
    }

    val inputFile = args(0)

    val spark = SparkSession.builder
      .appName("SmokeTest")
      .getOrCreate()

    /*
    Check in the file that there is a long and lat for each station in the json.
      - [x] check schema for double
      - [x] count where null == 0
    Confirm that each station id is listed only once.
      - [x] groupby station id, count having > 1 == len(0)
    Bikes Available should be a non-negaitve number
      - [ ] where(...)
    Docks Available should be a non-negaitve number
      - [ ] where(...)
    */

    val output = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(inputFile)
      .cache()
    val cw = AmazonCloudWatchClientBuilder.defaultClient
    runAssertions(output, cw, System.currentTimeMillis() / 1000)
  }
}

package com.tw.apps

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

  def runAssertions(df: DataFrame) = {
    assertThatStationIdsAreUnique(df)
    assertThatLatitudeAndLongitudeAreCorrectlyTyped(df)
    assertThatLatitudeAndLongitudeAreNeverNull(df)
    assertThatNoCountsAreNegative(df)

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
      .csv("./src/main/resources/example-output.csv")
      .cache()

    runAssertions(output)
  }
}
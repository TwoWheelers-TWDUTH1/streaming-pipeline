package com.tw.apps

import java.sql.Timestamp

import org.apache.spark.sql.functions.{to_timestamp, window}
import org.apache.spark.sql.{Dataset, SparkSession}

object StationAggregation {



  case class Window(start: Timestamp, end: Timestamp)

  case class StationDataWithWindow(
                                       bikes_available: Integer, docks_available: Integer,
                                       is_renting: Boolean, is_returning: Boolean,
                                       last_updated: Long,
                                       station_id: String, name: String,
                                       latitude: Double, longitude: Double,
                                       window: Window
                                     )

  def selectMostUpdatedStationData(ds: Dataset[StationData], spark: SparkSession): Dataset[StationData] = {

    import spark.implicits._

    ds
      .withColumn("last_updated", to_timestamp($"last_updated"))
      .withColumn("window", window($"last_updated", "10 minutes"))
      .withWatermark("last_updated", "10 minutes")
      .as[StationDataWithWindow]
      .groupByKey(r => (r.window, r.station_id))
      .reduceGroups((r1, r2) => if (r1.last_updated > r2.last_updated) r1 else r2)
      .map(_._2)
      .drop($"window")
      .withColumn("last_updated", $"last_updated".cast("long"))
      .as[StationData]
  }
}

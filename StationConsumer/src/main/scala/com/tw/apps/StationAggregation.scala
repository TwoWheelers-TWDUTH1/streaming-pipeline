package com.tw.apps

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{from_unixtime, to_timestamp}

object StationAggregation {
  def selectMostUpdatedStationData(ds: Dataset[StationData], spark: SparkSession): Dataset[StationData] = {

    import spark.implicits._

    ds
      .groupByKey(r=>r.station_id)
      .reduceGroups((r1,r2)=>if (r1.last_updated > r2.last_updated) r1 else r2)
      .map(_._2)
      .withColumn("timestamp", to_timestamp(from_unixtime($"last_updated")))
      .withWatermark("timestamp", "10 minutes")
      .drop("timestamp")
      .as[StationData]
  }
}

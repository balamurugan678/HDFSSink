package com.maple.quad

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class HDSFToHiveTest extends FlatSpec with Matchers with BeforeAndAfter {

  var spark: SparkContext = _
  var source_data: DataFrame = _
  before {
    val sparkConf = new SparkConf
    sparkConf.set("spark.sql.crossJoin.enabled", "true")
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[*]")
    }
    if (!sparkConf.contains("spark.app.name")) {
      sparkConf.setAppName("UnitTest-" + getClass.getName)
    }

  }

}

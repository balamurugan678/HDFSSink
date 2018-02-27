package com.maple.quad

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HDFSToHive {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("HDFSSink")

    val sparkContext = new SparkContext(sparkConf)

    val distFile = sparkContext.textFile(sparkContext.getConf.get("spark.configFileLocation"))

    val hiveContext = new HiveContext(sparkContext)

    val sourceDirectory = sparkContext.getConf.get("spark.sourceDirectory")
    val attunityAvroDataframe = hiveContext
      .read
      .format("com.databricks.spark.avro")
      .load(sourceDirectory)

    val columnNames = Seq("message.data.*", "message.headers.*")
    val attunityDF = attunityAvroDataframe.select(columnNames.head, columnNames.tail: _*)

    attunityDF.show()

    attunityDF.write
      .mode("append")
      .saveAsTable(sparkContext.getConf.get("spark.hiveTable"))

    val fs = FileSystem.get(sparkContext.hadoopConfiguration)
    val filePaths = fs.listStatus(new Path(sourceDirectory.substring(0, sourceDirectory.indexOf("*"))))
    filePaths.foreach(files => fs.rename(files.getPath, new Path(sparkContext.getConf.get("spark.processedDirectory"))))

  }

}

package com.maple.quad

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object HDFSToHive {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("HDFSSink")

    val sparkContext = new SparkContext(sparkConf)

    val fs = FileSystem.get(sparkContext.hadoopConfiguration)
    val hiveContext = new HiveContext(sparkContext)
    val columnNames = Seq("message.data.*", "message.headers.*")
    val sparkAvroFormat = "com.databricks.spark.avro"

    Source.fromFile(sparkContext.getConf.get("spark.configFileLocation"))
      .getLines
      .foreach(line => {
        val configList = line.split("~")

        val sourceFiles = fs.listStatus(new Path(configList(0)))
        sourceFiles.foreach(sourceFile => {
          val attunityAvroDataframe = hiveContext
            .read
            .format(sparkAvroFormat)
            .load(sourceFile.getPath().toString)

          attunityAvroDataframe.printSchema()

          val attunityDF = attunityAvroDataframe.select(columnNames.head, columnNames.tail: _*)
          attunityDF.show()
          attunityDF.write
            .mode("append")
            .format(sparkAvroFormat)
            .save(configList(1))

        })

        val filePaths = fs.listStatus(new Path(configList(0)))
        filePaths.foreach(files => fs.rename(files.getPath, new Path(configList(2))))
      })

  }

}

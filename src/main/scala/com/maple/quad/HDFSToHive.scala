package com.maple.quad

import org.apache.hadoop.fs.{FileStatus, FileSystem, FileUtil, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object HDFSToHive {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("HDFSSink")

    val sparkContext = new SparkContext(sparkConf)
    val hadoopConfig = sparkContext.hadoopConfiguration
    val hadoopFileSystem = FileSystem.get(hadoopConfig)
    val hiveContext = new HiveContext(sparkContext)
    val columnNames = Seq("message.data.*", "message.headers.*")
    val sparkAvroFormat = "com.databricks.spark.avro"

    Source.fromFile(sparkContext.getConf.get("spark.configFileLocation"))
      .getLines
      .foreach(line => {
        val configList = line.split("~")

        val sourceFiles = hadoopFileSystem.listStatus(new Path(configList(0)))
        sourceFiles.foreach(sourceFile => {
          val attunityAvroDataframe = hiveContext
            .read
            .format(sparkAvroFormat)
            .load(sourceFile.getPath().toString)

          val attunityUpperDF = attunityAvroDataframe.select(columnNames.head, columnNames.tail: _*)
            .withColumnRenamed("operation", "header__operation")
            .withColumnRenamed("changeSequence", "header__changeSequence")
            .withColumnRenamed("timestamp", "header__timestamp")
            .withColumnRenamed("streamPosition", "header__streamPosition")
            .withColumnRenamed("transactionId", "header__transactionId")

          val attunityDF = attunityUpperDF.toDF(attunityUpperDF.columns map (_.toLowerCase): _*)

          attunityDF.show()
          attunityDF.write
            .mode("append")
            .format(sparkAvroFormat)
            .save(configList(1))

        })

        val status: Array[FileStatus] = hadoopFileSystem.listStatus(new Path(configList(0)))
        status.foreach(files => {
          FileUtil.copy(hadoopFileSystem, files.getPath, hadoopFileSystem, new Path(configList(2)), true, hadoopConfig)
        }
        )

      })

  }

}

# HDFS Sink Service

* The HDFS Sink service works on Spark and acts as a connector between files stored in HDFS and Hive Table.
* The HDFS files are being read and the hive tables are identified from the config.
* The schema for the attunity's encapsulated message is known and the service takes advantage of that.
* The files are translated into Dataframes and the data is persisted onto the Hive tables.
* The last step in the service would be moving the files from the source directory to the target directory for idempotency.


## Where are the configuration variables

* The service is heavily configurable and below are the configuration parameters to run the job.
* The file name we use here is spark_config.txt but it can be anything as long as it has been supplied to the spark submit job via spark.configFileLocation.
* The source directory for the files, the sink HDFS location and the processed files directory are the config
* They are delimited by ~ and a row in the file represents a table.
* There could be multiple rows in the file and they can correspond to the multiple tables.

```
spark.sourceDirectory~spark.hdfsLocation~spark.processedDirectory
```


## How to build the application

* The application needs to be built using **MAVEN** and the below command should be run in any machine which has maven installed in it. Go to the directory of the code and execute the below maven command

```
cd /****CODE_DIRECTORY****/

mvn clean install
```

* Please refer -https://maven.apache.org/install.html if you haven't had maven installed in the machine


## How to run the application

* Once the previous step has been completed, we can run the application using the below command
* The three elements which are followed by the JAR name(movie-recommender.jar) are program arguments. The first element the movie id which needs the output recommendation, second one is scorethreshold and the third one is cooccurrence threshold.

```
cd /****CODE_DIRECTORY/target****/

spark-submit --class "com.maple.quad.HDFSToHive" --master local[4] --conf spark.configFileLocation=/***PATH_TO_THE_FILE***/spark_config.txt HDFS-Sink.jar
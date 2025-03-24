from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, split, from_unixtime, window, coalesce,substring
#from pyspark.sql.functions import window,endswith,when, coalesce
# Set the necessary variables
# The following links were used to determine the necessary packages to include:
# - https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html and 
# - https://github.com/OneCricketeer/docker-stacks/blob/master/hadoop-spark/spark-notebooks/kafka-sql.ipynb  

scala_version = '2.12'  
spark_version = '3.5.3'
bootstrap_servers = ['localhost:9092']
topic_name = 'abgabe-topic'
window_duration = '1 minute'
sliding_duration = '1 minute'

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.9.0',
    'org.mariadb.jdbc:mariadb-java-client:3.5.2'
]
#create Spark session 
spark = SparkSession.builder\
   .master("local")\
   .appName("kafka-example")\
   .config("spark.jars.packages", ",".join(packages))\
   .config("SQLConf.ADAPTIVE_EXECUTION_ENABLED.key", "false")\
   .config("spark.driver.host","127.0.0.1")\
   .config("spark.driver.bindAddress","127.0.0.1")\
   .getOrCreate()

#connect to Kafka ingest layer
kafkaDf = spark.readStream.format("kafka")\
  .option("kafka.bootstrap.servers", *bootstrap_servers)\
  .option("subscribe", topic_name)\
  .option("startingOffsets", "earliest")\
  .load()
print(kafkaDf.isStreaming)    # Returns True for DataFrames that have streaming sources
kafkaDf.printSchema()

#structure the column

#col_schema = ["line_no INTEGER","host STRING","time TIMESTAMP","method STRING","url STRING","response INTEGER","bytes INTEGER"]
#schema_str = ",".join(col_schema)
structured_df = kafkaDf.select(
    concat(col("topic"), lit(':'), col("partition").cast("string")).alias("topic_partition"),
    col("offset"),
    col("value").cast("string"),
    col("timestamp"),
    col("timestampType"),
    split(col("value").cast("string"),"|").getItem(0).alias("Request").cast("string"),
    split(col("value").cast("string"),"|").getItem(1).alias("METHOD").cast("string"),
    split(col("value").cast("string"),"|").getItem(2).alias("URI").cast("string"),
    split(col("value").cast("string"),"|").getItem(3).alias("QS").cast("string"),
    split(col("value").cast("string"),"|").getItem(4).alias("PID").cast("string"),
    split(col("value").cast("string"),"|").getItem(5).alias("CONN").cast("string"),
    split(col("value").cast("string"),"|").getItem(6).alias("SENT").cast("string"),
    split(col("value").cast("string"),"|").getItem(7).alias("REF").cast("string"),
    split(col("value").cast("string"),"|").getItem(8).alias("UA").cast("string"),
    split(col("value").cast("string"),"|").getItem(9).alias("JSID").cast("string"),
    split(col("value").cast("string"),"|").getItem(10).alias("DUR").cast("string"),
    split(col("value").cast("string"),"|").getItem(11).alias("ENV").cast("string"),
    split(col("value").cast("string"),"|").getItem(12).alias("FARM").cast("string"),
    split(col("value").cast("string"),"|").getItem(13).alias("SSL_PROTOCOL").cast("string"),
    split(col("value").cast("string"),"|").getItem(14).alias("SSL_CIPHER").cast("string")
)
    
#    split(col("value").cast("string"),",").getItem(0).alias("line_no").cast("integer"),
#    split(col("value").cast("string"),",").getItem(1).alias("host").cast("string"),
#    from_unixtime(split(col("value").cast("string"),",").getItem(2)).alias("time").cast("timestamp"),
#    split(col("value").cast("string"),",").getItem(3).alias("method").cast("string"),
#    split(col("value").cast("string"),",").getItem(4).alias("url").cast("VARCHAR(255)"),
#    split(col("value").cast("string"),",").getItem(5).alias("response").cast("integer"),
#    split(col("value").cast("string"),",").getItem(6).alias("bytes").cast("integer"),   


#filter and work with the data

#page_count_df = structured_df.filter(structured_df["METHOD"] == "GET")\
#     .filter(structured_df["URI"].endswith(".png"))\
#     .groupBy (window(
#        col("time"),
#        window_duration,
#        sliding_duration
#     ),
#    col("url")).count() \
#     .withColumnRenamed('window.start', 'window_start') \
#     .withColumnRenamed('window.end', 'window_end')\
#     .withColumn("url", coalesce(col("url"), lit("unknown")))
#page_count_df.printSchema()

pdf_count_df = structured_df.filter(structured_df["METHOD"] == "GET")\
    .filter(structured_df["URI"].endswith(".png\""))\
    .groupBy (col("URI")).count()
pdf_count_df.printSchema()
#Write into Maria DB

# Write to MariaDB 
dbUrl = 'jdbc:mysql://localhost:3306/kappa-view?permitMysqlScheme' # Use of jdbc:mysql://HOST/DATABASE?permitMysqlScheme is MANDATORY
dbOptions = {"user": "root", "password": "example", "driver": "org.mariadb.jdbc.Driver", "truncate": "true"}
dbSchema = 'pdfcount'

def saveToDatabase(batchDataframe, batchId):
    global dbUrl, dbSchema, dbOptions
    print(f"Writing batchID {batchId} to database @ {dbUrl}")
    batchDataframe.distinct().write.jdbc(dbUrl, dbSchema, "overwrite", dbOptions)


db_insert_stream = pdf_count_df \
   .select(col('URI'), col('count')) \
   .writeStream \
   .outputMode("complete") \
   .foreachBatch(saveToDatabase) \
   .start()
db_insert_stream.awaitTermination()

spark.stop()
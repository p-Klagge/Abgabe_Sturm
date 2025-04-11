from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, concat, lit, split, regexp_replace, monotonically_increasing_id

from pyspark.mllib.linalg import Vectors
from pyspark.mllib.clustering import StreamingKMeans
#from pyspark.sql.functions import window,endswith,when, coalesce
# Set the necessary variables
# The following links were used to determine the necessary packages to include:
# - https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html and 
# - https://github.com/OneCricketeer/docker-stacks/blob/master/hadoop-spark/spark-notebooks/kafka-sql.ipynb  

scala_version = '2.12'  
spark_version = '3.5.3'
bootstrap_servers = ['localhost:9092']
topic_name = 'abgabe-topic'
#window_duration = '1 minute'
#sliding_duration = '1 minute'

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

#InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
structured_df = kafkaDf.select(
    concat(col("topic"), lit(':'), col("partition").cast("string")).alias("topic_partition"),
    col("offset"),
    col("value").cast("string"),
    col("timestamp"),
    col("timestampType"),
    split(col("value").cast("string"),",").getItem(1).alias("InvoiceNo").cast("integer"),
    split(col("value").cast("string"),",").getItem(2).alias("StockCode").cast("integer"),
    split(col("value").cast("string"),",").getItem(3).alias("Description").cast("string"),
    split(col("value").cast("string"),",").getItem(4).alias("Quantity").cast("integer"),
    split(col("value").cast("string"),",").getItem(5).alias("InvoiceDate").cast("timestamp"),
    split(col("value").cast("string"),",").getItem(6).alias("UnitPrice").cast("float"),
    split(col("value").cast("string"),",").getItem(7).alias("CustomerID").cast("integer"),
    split(col("value").cast("string"),",").getItem(8).alias("Country").cast("string")
)
    

#filter and work with the data
# Remove rows with null values in the Description column and select none Kafka system related columns
# kept offset as a primary key for the db, since the data is synthetic and can have dublicates
# InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country
# Didnt drop dublicates, because of the way the streaming data is generated
stream_data_df = structured_df = structured_df.filter(col("Description").isNotNull()).select(
    col("offset"),
    col("InvoiceNo"),
    col("StockCode"),
    col("Description"),
    col("Quantity"),
    col("InvoiceDate"),
    col("UnitPrice"),
    col("CustomerID"),
    regexp_replace(col("Country"), "\n", "").alias("Country")
)


# Initialize the StreamingKMeans model
# k=5 because i magicly now the input data :)
# using a high decay factor of 0.9 because the way the stream is generated, 
# does not simulate a real stream thats changing over time
model = StreamingKMeans(k=5, decayFactor=0.9).setRandomCenters(2, 1.0, seed=1)

# Perform clustering on the batch data

def clusterWithStreamingKMeans(batchDataframe, batchId):
    print(f"Clustering batchId: {batchId} using StreamingKMeans")
    global model
    feature_columns = ["Quantity", "UnitPrice"]
    batchDataframe = batchDataframe.drop("Country")
    
    # Ensure the batchDataframe has the required columns
    if all(col in batchDataframe.columns for col in feature_columns):
        # Assemble features into a single vector column       
        # Collect the feature vectors as RDD
        feature_rdd = batchDataframe.select(feature_columns).rdd.map(
            lambda row: Vectors.dense([row["Quantity"], row["UnitPrice"]])
        )
        # Update the StreamingKMeans model with the current batch
        model.latestModel().update(feature_rdd, decayFactor=0.9, timeUnit="batches")

        # Predict the clusters for the current batch
        predictions_rdd = model.latestModel().predict(feature_rdd)
        
        # Combine the predictions with the original batchDataframe
        predictions_df = predictions_rdd.zipWithIndex().map(lambda x: Row(index=x[1], prediction=x[0])).toDF()
        batchDataframe_with_index = batchDataframe.withColumn("index", monotonically_increasing_id())
        prediction_df = batchDataframe_with_index.join(predictions_df, "index").drop("index")

        # Show the clustered data
        saveToDatabase(prediction_df, batchId, dbSchema1)

    else:
        print(f"Required columns {feature_columns} not found in batchDataframe for batchId: {batchId}")



# Write to MariaDB 
dbUrl = 'jdbc:mysql://localhost:3306/kappa-view?permitMysqlScheme' # Use of jdbc:mysql://HOST/DATABASE?permitMysqlScheme is MANDATORY
dbOptions = {"user": "root", "password": "example", "driver": "org.mariadb.jdbc.Driver"}
dbSchema1 = 'British_Online_Retail'
dbSchema2 = 'Other_Online_Retail'

def saveToDatabase(batchDataframe, batchId, dbSchema):
    global dbUrl, dbOptions
    print(f"Writing batchID {batchId} to database @ {dbUrl}")
    batchDataframe.distinct().write.jdbc(dbUrl, dbSchema, "overwrite", dbOptions)

# Split the data because we want to cluster the data from the UK and save the rest to a different table
def splitData(batchDataframe, batchId):
    batch1 = batchDataframe.filter(col("Country") == ("United Kingdom"))
    batch2 = batchDataframe.filter(col("Country") != ("United Kingdom"))

    saveToDatabase(batch2, batchId, dbSchema2)
    clusterWithStreamingKMeans(batch1, batchId)


db_insert_stream = stream_data_df \
   .select('*') \
   .writeStream \
   .outputMode("append") \
   .foreachBatch(splitData) \
   .trigger(processingTime='15 seconds') \
   .start()
db_insert_stream.awaitTermination()

spark.stop()
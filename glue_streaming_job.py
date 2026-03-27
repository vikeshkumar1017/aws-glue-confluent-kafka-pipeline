import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Initialize Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Credentials (Confluent Cloud)
bootstrap = "ENTER YOUR BOOTSTRAP ID"
api_key = "ENTER YOUR API KEY VALUE HERE"
api_secret = "ENTER YOUR API SECRET HERE"

schema = (StructType()
          .add("order_id", IntegerType())
          .add("item", StringType())
          .add("price", DoubleType())
          .add("timestamp", DoubleType()))

# 2. Read from Confluent Kafka
# Added startingOffsets to ensure it picks up data immediately
df = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", bootstrap)
  .option("kafka.security.protocol", "SASL_SSL")
  .option("kafka.sasl.mechanism", "PLAIN")
  .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";')
  .option("subscribe", "orders_stream")
  .option("startingOffsets", "earliest") 
  .load())

clean_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# 3. Write to S3 (The AWS Sink)
# Changed to JSON and AvailableNow for a guaranteed successful first test
query = (clean_df.writeStream
    .format("json") 
    .option("path", "s3://vicky-kafka-project-data/output/")
    .option("checkpointLocation", "s3://vicky-kafka-project-data/checkpoints/")
    .trigger(availableNow=True) 
    .start())

query.awaitTermination()
job.commit()

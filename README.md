# 🚀 AWS Glue & Confluent Kafka Streaming Pipeline

A serverless Data Engineering project migrating a local Kafka stream to a production-grade AWS architecture.

## 🏗️ Architecture
- **Producer:** Python script generating real-time order data.
- **Streaming:** Confluent Kafka (Cloud-native).
- **Processing:** AWS Glue 5.0 (PySpark 3.5.0) for real-time ingestion.
- **Storage:** Amazon S3 (Data Lake - Bronze Layer).
- **Analytics:** Amazon Athena (Serverless SQL).

## 🛠️ Key Technical Challenges Solved
- **Dependency Management:** Configured Glue 5.0 with specific JARs (`spark-sql-kafka`, `kafka-clients`, `commons-pool2`) to match Spark 3.5.0 requirements.
- **Security:** Implemented SASL/SSL authentication to securely bridge Confluent Cloud and AWS.
- **Data Integrity:** Handled Structured Streaming checkpoints in S3 to ensure fault-tolerant data processing.

## 🚀 How to Run
1. Upload the `.jar` files in the `/drivers` folder to S3.
2. Create an AWS Glue Job with the provided `glue_streaming_job.py`.
3. Set Job Parameters: `--extra-jars` and `--user-jars-first`.
4. Run the Athena SQL script to query the data lake.

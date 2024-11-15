import sys
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col, expr

class GlueKafkaJob:
    def __init__(self, bucket_name, config_key, topic_list_key, region='us-east-1'):
        self.bucket_name = bucket_name
        self.config_key = config_key
        self.topic_list_key = topic_list_key
        self.region = region
        self.s3_client = boto3.client('s3', region_name=self.region)
        self.config = self.load_config()
        self.job_properties = self.config["job_properties"]
        self.connection_config = self.config["connection_config"]
        self.connection_name = self.connection_config["connection_name"]
        self.vpc_id = self.connection_config["vpc_id"]
        self.security_group_id = self.connection_config["security_group_id"]
        self.subnet_id = self.connection_config["subnet_id"]
        self.bootstrap_servers = self.config["bootstrap_servers"]
        self.topics_tables = self.load_topic_list()

        # Initialize Spark and Glue contexts
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        self.job_name = args['JOB_NAME']
        self.sc = SparkContext()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)

    def load_config(self):
        config_json_content = self.read_s3_file(self.bucket_name, self.config_key)
        return json.loads(config_json_content)

    def load_topic_list(self):
        topic_list_content = self.read_s3_file(self.bucket_name, self.topic_list_key)
        return json.loads(topic_list_content)

    def read_s3_file(self, bucket_name, key):
        response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
        return response['Body'].read().decode('utf-8')

    def create_glue_kafka_connection(self):
        glue_client = boto3.client('glue', region_name=self.region)
        connection_properties = {
            'Name': self.connection_name,
            'ConnectionType': 'NETWORK',
            'ConnectionProperties': {
                'KAFKA_BOOTSTRAP_SERVERS': self.bootstrap_servers,
                'KAFKA_SSL_ENABLED': 'true',
                'KAFKA_SASL_MECHANISM': 'AWS_IAM',
                'KAFKA_SECURITY_PROTOCOL': 'SASL_SSL',
            },
            'PhysicalConnectionRequirements': {
                'SubnetId': self.subnet_id,
                'SecurityGroupIdList': [
                    self.security_group_id
                ],
                'AvailabilityZone': 'us-east-1a'
            }
        }

        response = glue_client.create_connection(
            ConnectionInput=connection_properties
        )
        return response

    def process_batch(self, data_frame, batchId, table_name):
        if data_frame.count() > 0:
            df = data_frame.withColumn("value", col("value").cast("string"))

            table_exists = False
            try:
                self.spark.table(table_name)
                table_exists = True
            except Exception as e:
                print(f"Table {table_name} does not exist. It will be created.")

            if table_exists:
                existing_df = self.spark.table(table_name)
                merged_df = df.alias("updates").join(
                    existing_df.alias("existing"),
                    (df["FileID"] == existing_df["FileID"]) & (df["Sequential_ID"] == existing_df["Sequential_ID"]),
                    "full_outer"
                ).select(
                    col("updates.*")
                )
                merged_df.write.format("iceberg").mode("append").saveAsTable(table_name)
            else:
                df.write.format("iceberg") \
                    .mode("overwrite") \
                    .option("path", f"{self.config['bucket_name']}/{self.config['database']}/{table_name}") \
                    .option("format-version", "2") \
                    .option("write.delete.mode", "copy-on-write") \
                    .option("write.update.mode", "merge-on-read") \
                    .option("write.merge.mode", "merge-on-read") \
                    .option("write.object-storage.enabled", "true") \
                    .partitionBy("data_directory", expr("days(last_processed_date)")) \
                    .saveAsTable(table_name)
                print(f"Table {table_name} has been created with the initial data and partitioned.")

    def run(self):
        self.job.init(self.job_name, self.job_properties)
        kafka_connection_response = self.create_glue_kafka_connection()
        print("Kafka connection created:", kafka_connection_response)

        for topic, table_name in self.topics_tables.items():
            kafka_df = self.glueContext.create_data_frame.from_options(
                connection_type="kafka",
                connection_options={
                    "connectionName": self.connection_name,
                    "classification": "json",
                    "startingOffsets": "earliest",
                    "topicName": topic,
                    "inferSchema": "true",
                    "kafka.bootstrap.servers": self.bootstrap_servers,
                    "typeOfData": "kafka"
                },
                transformation_ctx=f"dataframe_{topic.replace('.', '_')}"
            ).toDF()

            kafka_df.writeStream.foreachBatch(
                lambda df, batchId: self.process_batch(df, batchId, table_name)
            ).option(
                "checkpointLocation", f"{self.job_properties['TempDir']}/{self.job_name}/{topic.replace('.', '_')}/checkpoint/"
            ).start()

        self.spark.streams.awaitAnyTermination()
        self.job.commit()

# Example usage
if __name__ == "__main__":
    job = GlueKafkaJob(
        bucket_name='mp2appsrvdevshell',
        config_key='deploysetup/config.json',
        topic_list_key='deploysetup/topic_list.json',
        region='us-east-1'  # Specify your AWS region here
    )
    job.run()

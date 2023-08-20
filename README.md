# Event-Driven-S3-Glue-Transactional-Lake

Learn and Develop How to ingest data from S3 into Transactional Data lake through event driven approach using Glue and SQS queue and DLQ

# Batch oriented Pipeline 
![image](https://github.com/soumilshah1995/Event-Driven-S3-Glue-Transactional-Lake/assets/39345855/71571771-49d4-4ccc-ba8d-a8a91e4a643f)

# Stream oriented pipeline
![sourcer drawio](https://github.com/soumilshah1995/Event-Driven-S3-Glue-Transactional-Lake/assets/39345855/fa10cd01-3f95-4f05-9746-9149b3ac4de4)



# Step by Step guides 
* https://www.linkedin.com/pulse/mastering-event-driven-data-ingestion-guide-using-glue-soumil-shah

* 
# SQS Policy 
```
{
  "Version": "2012-10-17",
  "Id": "example-ID",
  "Statement": [
    {
      "Sid": "example-statement-ID",
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": "SQS:SendMessage",
      "Resource": "<QUEUE ARN>",
      "Condition": {
        "StringEquals": {
          "aws:SourceAccount": "<AWS ACCOUNT ID>"
        },
        "ArnLike": {
          "aws:SourceArn": "arn:aws:s3:*:*:<S# BUCKET>"
        }
      }
    }
  ]
}
```


# Python File That generates data into RAW ZONE 
* Make sure to set ENV files 
```
try:
    import datetime
    import json
    import random
    import boto3
    import os
    import uuid
    import time
    from datetime import datetime
    from faker import Faker
    from dotenv import load_dotenv

    load_dotenv("../dev.env")
except Exception as e:
    pass



class AWSS3(object):
    """Helper class to which add functionality on top of boto3 """

    def __init__(self, bucket, aws_access_key_id, aws_secret_access_key, region_name):

        self.BucketName = bucket
        self.client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    def put_files(self, Response=None, Key=None):
        """
        Put the File on S3
        :return: Bool
        """
        try:

            response = self.client.put_object(
                ACL="private", Body=Response, Bucket=self.BucketName, Key=Key
            )
            return "ok"
        except Exception as e:
            print("Error : {} ".format(e))
            return "error"

    def item_exists(self, Key):
        """Given key check if the items exists on AWS S3 """
        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return True
        except Exception as e:
            return False

    def get_item(self, Key):

        """Gets the Bytes Data from AWS S3 """

        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return response_new["Body"].read()

        except Exception as e:
            print("Error :{}".format(e))
            return False

    def find_one_update(self, data=None, key=None):

        """
        This checks if Key is on S3 if it is return the data from s3
        else store on s3 and return it
        """

        flag = self.item_exists(Key=key)

        if flag:
            data = self.get_item(Key=key)
            return data

        else:
            self.put_files(Key=key, Response=data)
            return data

    def delete_object(self, Key):

        response = self.client.delete_object(Bucket=self.BucketName, Key=Key, )
        return response

    def get_all_keys(self, Prefix=""):

        """
        :param Prefix: Prefix string
        :return: Keys List
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix)

            tmp = []

            for page in pages:
                for obj in page["Contents"]:
                    tmp.append(obj["Key"])

            return tmp
        except Exception as e:
            return []

    def print_tree(self):
        keys = self.get_all_keys()
        for key in keys:
            print(key)
        return None

    def __repr__(self):
        return "AWS S3 Helper class "


global faker
global helper

faker = Faker()
helper = AWSS3(
    aws_access_key_id=os.getenv("DEV_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("DEV_SECRET_KEY"),
    region_name=os.getenv("DEV_REGION"),
    bucket=os.getenv("BUCKET")
)


def run():
    for i in range(1, 5):
        order_id = uuid.uuid4().__str__()
        customer_id = uuid.uuid4().__str__()

        orders = {
            "orderid": order_id,
            "customer_id": customer_id,
            "ts": datetime.now().isoformat().__str__(),
            "order_value": random.randint(10, 1000).__str__(),
            "priority": random.choice(["LOW", "MEDIUM", "URGENT"])

        }
        print(orders)

        # Convert dictionary to JSON string and then to bytes
        order_data = json.dumps(orders).encode("utf-8")

        # # Pass the bytes data to the put_files method
        # helper.put_files(Response=order_data, Key=f'raw/orders/{uuid.uuid4().__str__()}.json')

        customers = {
            "customer_id": customer_id,
            "name": faker.name(),
            "state": faker.state(),
            "city": faker.city(),
            "email": faker.email(),
            "ts": datetime.now().isoformat().__str__()
            # , "new_col":"test"
        }
        print(customers)

        # Convert dictionary to JSON string and then to bytes
        customer_data = json.dumps(customers).encode("utf-8")

        #Pass the bytes data to the put_files method
        helper.put_files(Response=customer_data, Key=f'raw/customers/{uuid.uuid4().__str__()}.json')


if __name__ == "__main__":
    run()


```

# Glue Job that poll queue and process data 
* Make sure to set Job paramters
```
--conf  |  spark.serializer=org.apache.spark.serializer.KryoSerializer  --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog --conf spark.sql.legacy.pathOptionBehavior.enabled=true --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
--datalake-formats | hudi
```
# Glue Job Batch Orienbted 
```
try:
    import os, uuid, sys, boto3, time, sys, json
    from pyspark.sql.functions import lit, udf
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
except Exception as e:
    print("Modules are missing : {} ".format(e))

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

spark = (SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
         .config('spark.sql.hive.convertMetastoreParquet', 'false') \
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate())

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()
job.init(args["JOB_NAME"], args)


class Poller:
    def __init__(self, queue_url):
        self.queue_url = queue_url
        self.sqs_client = boto3.client('sqs'
                                       )
        self.batch_size = 10
        self.messages_to_delete = []

    def get_messages(self, batch_size):
        response = self.sqs_client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=batch_size,
            WaitTimeSeconds=20
        )

        if 'Messages' in response:
            messages = response['Messages']
            for message in messages:
                self.messages_to_delete.append({
                    'ReceiptHandle': message['ReceiptHandle'],
                    'Body': message['Body']
                })
            return messages
        else:
            return []

    def commit(self):
        for message in self.messages_to_delete:
            self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
        self.messages_to_delete = []


def read_data_s3(path, format):
    if format == "parquet" or format == "json":
        glue_df = glueContext.create_dynamic_frame.from_options(
            format_options={},
            connection_type="s3",
            format=format,
            connection_options={
                "paths": path,
                "recurse": True,
            },
            transformation_ctx="job_glue",
        )

        spark_df = glue_df.toDF()

        print(spark_df.show())

        return spark_df


def upsert_hudi_table(glue_database, table_name, record_id, precomb_key, table_type, spark_df, partition_fields,
                      enable_partition, enable_cleaner, enable_hive_sync, enable_clustering,
                      enable_meta_data_indexing,
                      use_sql_transformer, sql_transformer_query,
                      target_path, index_type, method='upsert', clustering_column='default'):
    """
    Upserts a dataframe into a Hudi table.

    Args:
        glue_database (str): The name of the glue database.
        table_name (str): The name of the Hudi table.
        record_id (str): The name of the field in the dataframe that will be used as the record key.
        precomb_key (str): The name of the field in the dataframe that will be used for pre-combine.
        table_type (str): The Hudi table type (e.g., COPY_ON_WRITE, MERGE_ON_READ).
        spark_df (pyspark.sql.DataFrame): The dataframe to upsert.
        partition_fields this is used to parrtition data
        enable_partition (bool): Whether or not to enable partitioning.
        enable_cleaner (bool): Whether or not to enable data cleaning.
        enable_hive_sync (bool): Whether or not to enable syncing with Hive.
        use_sql_transformer (bool): Whether or not to use SQL to transform the dataframe before upserting.
        sql_transformer_query (str): The SQL query to use for data transformation.
        target_path (str): The path to the target Hudi table.
        method (str): The Hudi write method to use (default is 'upsert').
        index_type : BLOOM or GLOBAL_BLOOM
    Returns:
        None
    """
    # These are the basic settings for the Hoodie table
    hudi_final_settings = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.operation": method,
        "hoodie.datasource.write.recordkey.field": record_id,
        "hoodie.datasource.write.precombine.field": precomb_key,
    }

    # These settings enable syncing with Hive
    hudi_hive_sync_settings = {
        "hoodie.parquet.compression.codec": "gzip",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": glue_database,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
    }

    # These settings enable automatic cleaning of old data
    hudi_cleaner_options = {
        "hoodie.clean.automatic": "true",
        "hoodie.clean.async": "true",
        "hoodie.cleaner.policy": 'KEEP_LATEST_FILE_VERSIONS',
        "hoodie.cleaner.fileversions.retained": "3",
        "hoodie-conf hoodie.cleaner.parallelism": '200',
        'hoodie.cleaner.commits.retained': 5
    }

    # These settings enable partitioning of the data
    partition_settings = {
        "hoodie.datasource.write.partitionpath.field": partition_fields,
        "hoodie.datasource.hive_sync.partition_fields": partition_fields,
        "hoodie.datasource.write.hive_style_partitioning": "true",
    }

    hudi_clustering = {
        "hoodie.clustering.execution.strategy.class": "org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy",
        "hoodie.clustering.inline": "true",
        "hoodie.clustering.plan.strategy.sort.columns": clustering_column,
        "hoodie.clustering.plan.strategy.target.file.max.bytes": "1073741824",
        "hoodie.clustering.plan.strategy.small.file.limit": "629145600"
    }

    # Define a dictionary with the index settings for Hudi
    hudi_index_settings = {
        "hoodie.index.type": index_type,  # Specify the index type for Hudi
    }

    # Define a dictionary with the Fiel Size
    hudi_file_size = {
        "hoodie.parquet.max.file.size": 512 * 1024 * 1024,  # 512MB
        "hoodie.parquet.small.file.limit": 104857600,  # 100MB
    }

    hudi_meta_data_indexing = {
        "hoodie.metadata.enable": "true",
        "hoodie.metadata.index.async": "false",
        "hoodie.metadata.index.column.stats.enable": "true",
        "hoodie.metadata.index.check.timeout.seconds": "60",
        "hoodie.write.concurrency.mode": "optimistic_concurrency_control",
        "hoodie.write.lock.provider": "org.apache.hudi.client.transaction.lock.InProcessLockProvider"
    }

    if enable_meta_data_indexing == True or enable_meta_data_indexing == "True" or enable_meta_data_indexing == "true":
        for key, value in hudi_meta_data_indexing.items():
            hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    if enable_clustering == True or enable_clustering == "True" or enable_clustering == "true":
        for key, value in hudi_clustering.items():
            hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    # Add the Hudi index settings to the final settings dictionary
    for key, value in hudi_index_settings.items():
        hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    for key, value in hudi_file_size.items():
        hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    # If partitioning is enabled, add the partition settings to the final settings
    if enable_partition == "True" or enable_partition == "true" or enable_partition == True:
        for key, value in partition_settings.items(): hudi_final_settings[key] = value

    # If data cleaning is enabled, add the cleaner options to the final settings
    if enable_cleaner == "True" or enable_cleaner == "true" or enable_cleaner == True:
        for key, value in hudi_cleaner_options.items(): hudi_final_settings[key] = value

    # If Hive syncing is enabled, add the Hive sync settings to the final settings
    if enable_hive_sync == "True" or enable_hive_sync == "true" or enable_hive_sync == True:
        for key, value in hudi_hive_sync_settings.items(): hudi_final_settings[key] = value

    # If there is data to write, apply any SQL transformations and write to the target path
    if spark_df.count() > 0:
        if use_sql_transformer == "True" or use_sql_transformer == "true" or use_sql_transformer == True:
            spark_df.createOrReplaceTempView("temp")
            spark_df = spark.sql(sql_transformer_query)

        spark_df.write.format("hudi"). \
            options(**hudi_final_settings). \
            mode("append"). \
            save(target_path)


def process_message(messages, file_format='json'):
    try:
        batch_files = []

        for message in messages:
            payload = json.loads(message['Body'])
            records = payload['Records']
            s3_files = [f"s3://{record['s3']['bucket']['name']}/{record['s3']['object']['key']}" for record in records]
            for item in s3_files: batch_files.append(item)

        if batch_files != []:
            spark_df = read_data_s3(
                path=batch_files,
                format=file_format
            )
            print("**************")
            spark_df.show()
            print("**************")

            upsert_hudi_table(
                glue_database="hudidb",
                table_name="customers",
                record_id="customer_id",
                precomb_key="ts",
                table_type='COPY_ON_WRITE',
                partition_fields="state",
                method='upsert',
                index_type='BLOOM',
                enable_partition=True,
                enable_cleaner=True,
                enable_hive_sync=False,
                enable_clustering=False,
                clustering_column='default',
                enable_meta_data_indexing='true',
                use_sql_transformer=False,
                sql_transformer_query='default',
                target_path="s3://jt-datateam-sandbox-qa-dev/silver/table_name=customers/",
                spark_df=spark_df,
            )

    except Exception as e:
        print("Error processing message:", e)
        raise Exception("Error processing message:", e)


def main():
    queue_url = 'QUEUE URL GOES HERE'

    poller = Poller(queue_url)

    while True:
        messages = poller.get_messages(poller.batch_size)
        if not messages:
            print("No messages to process. Exiting.")
            break
        else:
            process_message(messages)

        poller.commit()


if __name__ == "__main__":
    main()

```

# Glue job Stream oriented processing 
```

try:
    import os, uuid, sys, boto3, time, sys, json
    from pyspark.sql.functions import lit, udf
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from time import sleep
except Exception as e:
    print("Modules are missing : {} ".format(e))

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

spark = (SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
         .config('spark.sql.hive.convertMetastoreParquet', 'false') \
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate())

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()
job.init(args["JOB_NAME"], args)


class Poller:
    def __init__(self, queue_url):
        self.queue_url = queue_url
        self.sqs_client = boto3.client('sqs'
                                       )
        self.batch_size = 10
        self.messages_to_delete = []

    def get_messages(self, batch_size):
        response = self.sqs_client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=batch_size,
            WaitTimeSeconds=20
        )

        if 'Messages' in response:
            messages = response['Messages']
            for message in messages:
                self.messages_to_delete.append({
                    'ReceiptHandle': message['ReceiptHandle'],
                    'Body': message['Body']
                })
            return messages
        else:
            return []

    def commit(self):
        for message in self.messages_to_delete:
            self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
        self.messages_to_delete = []


def read_data_s3(path, format):
    if format == "parquet" or format == "json":
        glue_df = glueContext.create_dynamic_frame.from_options(
            format_options={},
            connection_type="s3",
            format=format,
            connection_options={
                "paths": path,
                "recurse": True,
            },
            transformation_ctx="job_glue",
        )

        spark_df = glue_df.toDF()

        print(spark_df.show())

        return spark_df


def upsert_hudi_table(glue_database, table_name, record_id, precomb_key, table_type, spark_df, partition_fields,
                      enable_partition, enable_cleaner, enable_hive_sync, enable_clustering,
                      enable_meta_data_indexing,
                      use_sql_transformer, sql_transformer_query,
                      target_path, index_type, method='upsert', clustering_column='default'):
    """
    Upserts a dataframe into a Hudi table.

    Args:
        glue_database (str): The name of the glue database.
        table_name (str): The name of the Hudi table.
        record_id (str): The name of the field in the dataframe that will be used as the record key.
        precomb_key (str): The name of the field in the dataframe that will be used for pre-combine.
        table_type (str): The Hudi table type (e.g., COPY_ON_WRITE, MERGE_ON_READ).
        spark_df (pyspark.sql.DataFrame): The dataframe to upsert.
        partition_fields this is used to parrtition data
        enable_partition (bool): Whether or not to enable partitioning.
        enable_cleaner (bool): Whether or not to enable data cleaning.
        enable_hive_sync (bool): Whether or not to enable syncing with Hive.
        use_sql_transformer (bool): Whether or not to use SQL to transform the dataframe before upserting.
        sql_transformer_query (str): The SQL query to use for data transformation.
        target_path (str): The path to the target Hudi table.
        method (str): The Hudi write method to use (default is 'upsert').
        index_type : BLOOM or GLOBAL_BLOOM
    Returns:
        None
    """
    # These are the basic settings for the Hoodie table
    hudi_final_settings = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.operation": method,
        "hoodie.datasource.write.recordkey.field": record_id,
        "hoodie.datasource.write.precombine.field": precomb_key,
    }

    # These settings enable syncing with Hive
    hudi_hive_sync_settings = {
        "hoodie.parquet.compression.codec": "gzip",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": glue_database,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
    }

    # These settings enable automatic cleaning of old data
    hudi_cleaner_options = {
        "hoodie.clean.automatic": "true",
        "hoodie.clean.async": "true",
        "hoodie.cleaner.policy": 'KEEP_LATEST_FILE_VERSIONS',
        "hoodie.cleaner.fileversions.retained": "3",
        "hoodie-conf hoodie.cleaner.parallelism": '200',
        'hoodie.cleaner.commits.retained': 5
    }

    # These settings enable partitioning of the data
    partition_settings = {
        "hoodie.datasource.write.partitionpath.field": partition_fields,
        "hoodie.datasource.hive_sync.partition_fields": partition_fields,
        "hoodie.datasource.write.hive_style_partitioning": "true",
    }

    hudi_clustering = {
        "hoodie.clustering.execution.strategy.class": "org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy",
        "hoodie.clustering.inline": "true",
        "hoodie.clustering.plan.strategy.sort.columns": clustering_column,
        "hoodie.clustering.plan.strategy.target.file.max.bytes": "1073741824",
        "hoodie.clustering.plan.strategy.small.file.limit": "629145600"
    }

    # Define a dictionary with the index settings for Hudi
    hudi_index_settings = {
        "hoodie.index.type": index_type,  # Specify the index type for Hudi
    }

    # Define a dictionary with the Fiel Size
    hudi_file_size = {
        "hoodie.parquet.max.file.size": 512 * 1024 * 1024,  # 512MB
        "hoodie.parquet.small.file.limit": 104857600,  # 100MB
    }

    hudi_meta_data_indexing = {
        "hoodie.metadata.enable": "true",
        "hoodie.metadata.index.async": "false",
        "hoodie.metadata.index.column.stats.enable": "true",
        "hoodie.metadata.index.check.timeout.seconds": "60",
        "hoodie.write.concurrency.mode": "optimistic_concurrency_control",
        "hoodie.write.lock.provider": "org.apache.hudi.client.transaction.lock.InProcessLockProvider"
    }

    if enable_meta_data_indexing == True or enable_meta_data_indexing == "True" or enable_meta_data_indexing == "true":
        for key, value in hudi_meta_data_indexing.items():
            hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    if enable_clustering == True or enable_clustering == "True" or enable_clustering == "true":
        for key, value in hudi_clustering.items():
            hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    # Add the Hudi index settings to the final settings dictionary
    for key, value in hudi_index_settings.items():
        hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    for key, value in hudi_file_size.items():
        hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    # If partitioning is enabled, add the partition settings to the final settings
    if enable_partition == "True" or enable_partition == "true" or enable_partition == True:
        for key, value in partition_settings.items(): hudi_final_settings[key] = value

    # If data cleaning is enabled, add the cleaner options to the final settings
    if enable_cleaner == "True" or enable_cleaner == "true" or enable_cleaner == True:
        for key, value in hudi_cleaner_options.items(): hudi_final_settings[key] = value

    # If Hive syncing is enabled, add the Hive sync settings to the final settings
    if enable_hive_sync == "True" or enable_hive_sync == "true" or enable_hive_sync == True:
        for key, value in hudi_hive_sync_settings.items(): hudi_final_settings[key] = value

    # If there is data to write, apply any SQL transformations and write to the target path
    if spark_df.count() > 0:
        if use_sql_transformer == "True" or use_sql_transformer == "true" or use_sql_transformer == True:
            spark_df.createOrReplaceTempView("temp")
            spark_df = spark.sql(sql_transformer_query)

        spark_df.write.format("hudi"). \
            options(**hudi_final_settings). \
            mode("append"). \
            save(target_path)


def process_message(messages, file_format='json'):
    try:
        batch_files = []

        for message in messages:
            payload = json.loads(message['Body'])
            records = payload['Records']
            s3_files = [f"s3://{record['s3']['bucket']['name']}/{record['s3']['object']['key']}" for record in records]
            for item in s3_files: batch_files.append(item)

        if batch_files != []:
            spark_df = read_data_s3(
                path=batch_files,
                format=file_format
            )
            print("**************")
            spark_df.show()
            print("**************")

            upsert_hudi_table(
                glue_database="hudidb",
                table_name="customers",
                record_id="customer_id",
                precomb_key="ts",
                table_type='COPY_ON_WRITE',
                partition_fields="state",
                method='upsert',
                index_type='BLOOM',
                enable_partition=True,
                enable_cleaner=True,
                enable_hive_sync=False,
                enable_clustering=False,
                clustering_column='default',
                enable_meta_data_indexing='true',
                use_sql_transformer=False,
                sql_transformer_query='default',
                target_path="s3://jt-datateam-sandbox-qa-dev/silver/table_name=customers/",
                spark_df=spark_df,
            )

    except Exception as e:
        print("Error processing message:", e)
        raise Exception("Error processing message:", e)


def main():
    queue_url = 'XXX'

    poller = Poller(queue_url)

    while True:
        messages = poller.get_messages(poller.batch_size)
        if not messages:
            print("No messages to process.")
            sleep(0.2)
            pass
        else:
            process_message(messages)

        poller.commit()


if __name__ == "__main__":
    main()


```

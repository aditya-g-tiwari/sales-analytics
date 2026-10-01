import sys
import json
import boto3

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, from_json

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

secret_name = "secret_dea"
secrets_client = boto3.client("secretsmanager", region_name="us-east-1")
secret_response = secrets_client.get_secret_value(SecretId=secret_name)
secret = json.loads(secret_response["SecretString"])
username = secret["username"].strip()
password = secret["password"].strip()

jdbc_url = (
    "jdbc:postgresql://"
    "dea.cgyi97rb4alr.us-east-1.rds.amazonaws.com:"
    "5432/dea_analytics_dev"
)

tables = {
    "leads": {
        "table": "raw.leads_raw",
        "target": "s3://sales-analytics-aditya-task-2/raw/leads/",
        "parse_json": True
    },
    "lead_activities": {
        "table": "raw.lead_activites_raw",
        "target": "s3://sales-analytics-aditya-task-2/raw/lead_activities/",
        "parse_json": True
    },
    "custom_activities": {
        "table": "raw.custom_activites_raw",
        "target": "s3://sales-analytics-aditya-task-2/raw/custom_activities/",
        "parse_json": False
    },
    "close_crm_users": {
        "table": "raw.close_crm_users_raw",
        "target": "s3://sales-analytics-aditya-task-2/raw/close_crm_users/",
        "parse_json": False
    }
}

for name, config in tables.items():
    query = f"""
    (
        SELECT
            raw_data,
            insert_date
        FROM {config["table"]}
    ) src
    """
    df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", username)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
    )
    if config["parse_json"]:
        json_schema = (
            spark.read.json(
                df.select("raw_data")
                .rdd
                .map(lambda row: row["raw_data"])
            )
            .schema
        )
        df = df.withColumn(
            "raw_data",
            from_json(col("raw_data"), json_schema)
        )
    (
        df.write
        .mode("overwrite")
        .json(config["target"])
    )

job.commit()

import os
from dotenv import load_dotenv
import shutil
import time
from datetime import datetime, timedelta

import boto3
import duckdb

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

load_dotenv()

'''
Downloads an entire folder from an S3 bucket (in this case MinIO) into Airflow’s local directory
'''
def get_s3_folder(
    s3_bucket, s3_folder, local_folder='/opt/airflow/temp/s3folder'
    ):
    #Connect to minio
    s3 = boto3.resource(
            service_name = "s3",
            endpoint_url = "http://minio:9000",
            aws_access_key_id = os.getenv("MINIO_ACCESS_KEY")
            aws_secret_access_key = os.getenv("MINIO_SECRET_KEY")
            region_name = "us-east-1"
    )
    #Access the bucket
    bucket = s3.bucket(s3_bucket)
    
    local_path = o.path.join(local_folder, s3_folder)

    if os.path.exists(local_path):
        shutil.rmtree(local_path)
    #Download objects from S3 to local
    for obj in bucket.objects.filter(Prefix = s3_folder):
        target = os.path.join(local_path, os.path.relpath(obj.key, s3_folder))
        os.makedirs(os.path.dirname(target), exist_ok = True)
        bucket.download_file(obj.key, taget)
        print(f"Downloaded {obj.key} to {target}")

with DAG(
    "user_analytics_dag",
    description = "A dag to pull user data and movie review data \
            to analyze their behavior",
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2025, 11, 17),
    catchup = False
    ) as dag:
    bucket = "user-analytics"

    create_s3_bucket = S3CreateBucketOperator(
        task_id = "create_s3_bucket", bucket_name = bucket
    ) 

    movie_review_to_s3 = SqlToS3Operator(
        task_id = "movie_review_to_s3",
        sql_conn_id = "postgres_default",
        query = "SELECT * FROM retail.movie_review",
        s3_bucket = bucket,
        s3_key = "raw/user_purchase/user_purchase.csv",
        replace = True
    )

    user_purchase_to_s3 = SqlToS3Operator(
        task_id = "user_purchase_to_s3",
        sql_conn_id = "postgres_default",
        query = "SELECT * FROM retail.user_purchase",
        s3_bucket = bucket,
        s3_key = "raw/user_purchase/user_purchase.csv",
        replace=True,
    )

    movie_classifier = BashOperator(
        task_id = "movie_classifier",
        bash_command = "python /opt/airflow/dags/scripts/pyspark/text_classification.py"
    )

    get_movie_review_to_warehouse = PythonOperator(
        task_id="get_movie_review_to_warehouse",
        python_callable=get_s3_folder,
        op_kwargs={
            "s3_bucket": "user-analytics",
            "s3_folder": "clean/movie_review",
        },
    )

    get_user_purchase_to_warehouse = PythonOperator(
        task_id="get_user_purchase_to_warehouse",
        python_callable=get_s3_folder,
        op_kwargs={
            "s3_bucket": "user-analytics",
            "s3_folder": "raw/user_purchase",
        },
    )

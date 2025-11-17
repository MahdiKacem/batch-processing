#Todo : implement real sentiment analysis model

from pyspark.sql import SparkSession
from pyspark.ml.feature import StopWordsRemover, Tokenizer
from pyspark.sql.functions import array_contains, lit

import argparse
import dotenv
import os

load_dotenv()

def text_classifier(
        input_loc: str, output_loc: str, run_id: str
        ) -> None:
   
    #Read input
    df_raw = spark.read.option("header", True)\
                       .csv(input_loc)
    #Tokenize text
    tokenizer = Tokenizer(
        inputCol = "review_string",
        outputCol = "review_tokens"
    )
    df_tokens = tokenizer.transform(df_raw).select("cid", "review_tokens")
    
    #Remove stop words

    remover = StopWordsRemover(
        inputCol = "review_tokens", 
        outputCol = "review_clean"
    )

    df_clean = remover.transform(df_tokens).select("cid", "review_clean")
    
    """
    We use a dummy function for testing, it marks reviews having the text 
    "good" as positive and the rest as negative 
    """
    df_good = df_clean.select("cid",
                             array_contains(df_clean.review_clean, "good").alias("positive_review")
    )
    df_final = df_good.wtihColumn("insert_date", lit(run_id))

    df_final.write.mode("overwrite")\
                  .parquet(output_loc)

if name == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=str,
        help="HDFS input",
        default="s3a://user-analytics/raw/movie_review.csv",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="HDFS output",
        default="s3a://user-analytics/clean/movie_review",
    )
    parser.add_argument(
        "--run-id", type=str, help="run id", default="2024-05-05"
    )
    args = parser.parse_args()

    spark = ( SparkSession.builder.appName("user-analytics-spark").
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2,org.postgresql:postgresql:42.7.3",
        )
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.region", "us-east-1")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )

    random_text_classifier(
        input_loc = args.input, output_loc = args.output, run_id = args.run_id
    )

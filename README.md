# Batch processing: User Behavior Analytics

## Overview  
This project simulates a **user behavior analytics company** that processes datato build a consolidated analytical table called **`user_behavior_metric`**.

The goal is to design an end-to-end **data pipeline** using (Spark, Airflow, object storage, PostgreSQL, etc.) to compute metrics used by analysts and BI dashboards.

## Objectives

1. **Ingest Multi-Source Data**
   - Extract user purchase and movie review data from an OLTP database (`user_purchase` and `movie_review` tables in PostgreSQL).  

2. **Transform Data**
   - Process movie review text using Spark to tokenize, remove stop words, and detect positive sentiment.  
   - Aggregate purchase metrics (e.g., total amount spent, number of purchases).  
   - Combine transactional and review data to compute behavioral metrics for each user.

3. **Load into Analytical Table**
   - Populate the `user_behavior_metric` table (OLAP) in PostgreSQL.  
   - Provide clean, ready-to-use data for dashboards and analysts.

4. **Automate Workflow**
   - Schedule and orchestrate all tasks using **Airflow**.  
   - Ensure daily batch updates and reproducibility of metrics.


## Architecture & Components

The pipeline is **Dockerized** and leverages the following components:

**Airflow** Orchestration of the ETL workflow and scheduling of DAGs. 
**Spark** Distributed processing of movie review text and aggregation of user metrics. 
**MinIO** S3-compatible object storage to store daily movie review files. 
**PostgreSQL** OLTP database (`user_purchase`, `movie_review`) and OLAP warehouse (`user_behavior_metric`). 
**Quarto + Plotly** Converts processed metrics into dashboards and HTML reports. 

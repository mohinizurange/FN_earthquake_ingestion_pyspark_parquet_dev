from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'M_Airflow',
    'retrieves': 2,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 11, 9),
}

# Define DAG
with DAG(
        'Airflow_submit_job',
        default_args=default_args,
        schedule_interval='0 10 * * *',  # This sets the DAG to run daily at 10 AM
        catchup=False,
) as dag:

    # Step 2: Submit PySpark Job with BigQuery connector JAR
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        project_id="spark-learning-43150",
        region="us-central1",
        job={
            "reference": {"job_id": "earthquake_pyspark_job"},
            "placement": {"cluster_name": "dataproc-cluster"},
            "pyspark_job": {
                "main_python_file_uri": "gs://earthquake_analysis_buck/pysaprk/pyspark_code/earthquake_pipeline_code_pyspark_fn.py",
                "args": [
                    "--api_url", "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson",
                    "--pipeline_nm", "monthly"
                ],
                "python_file_uris": [
                    "gs://earthquake_analysis_buck/pysaprk/pyspark_code/utility.py",
                    "gs://earthquake_analysis_buck/pysaprk/pyspark_code/config.py"
                ],
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.21.0.jar"
                ]
            }
        },
        gcp_conn_id="gcp_connection",
    )

    # Define task dependencies
    submit_pyspark_job

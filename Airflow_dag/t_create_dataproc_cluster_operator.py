from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'M_Airflow',
    'retrives': 2,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 11, 10),
}

# Define DAG
with DAG(
        'Airflow_operators_create_dataproc_cluster',
        default_args=default_args,
        schedule_interval='0 10 * * *',  # This sets the DAG to run daily at 10 AM
        catchup=False,
) as dag:
    # Step 1: Create Dataproc Cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id="spark-learning-43150",
        region="us-central1",
        cluster_name="dataproc-cluster",
        cluster_config={
            "config_bucket": "earthquake-dp_temp_bk",
            "gce_cluster_config": {
                "zone_uri": "us-central1-a",
                "metadata": {
                    "bigquery-connector-version": "1.2.0",
                    "spark-bigquery-connector-version": "0.21.0",
                    "JARS": "https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.21.0.jar,https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.24/postgresql-42.2.24.jar"
                },
                "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
                "tags": ["pyspark"]
            },
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "e2-standard-2",
                "disk_config": {"boot_disk_size_gb": 100}
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "e2-standard-2",
                "disk_config": {"boot_disk_size_gb": 100}
            },
            "software_config": {
                "image_version": "2.0-debian10",
                "optional_components": ["JUPYTER"]
            },
            "endpoint_config": {
                "enable_http_port_access": True
            }
        },
        use_if_exists=True,
        delete_on_error=True
    )

    # Define task dependencies
    create_cluster

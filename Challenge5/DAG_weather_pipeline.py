from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import ExternalPythonOperator

with DAG(
    dag_id="weather_alert_pipeline",
    schedule_interval="0 8 * * *",  # Run once every day at 8 AM UTC
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["weather", "gcp"],
) as dag:
    
    # Task to run the Python script from a file.
    # The file "weather_script.py" should be placed in the dags folder.
    run_weather_script = ExternalPythonOperator(
        task_id="run_weather_script",
        python=f"/usr/bin/python3",
        script_path="/home/airflow/gcs/dags/weather_script.py",
        # Pass the API key and other variables as environment variables
        env={
            "PROJECT_ID": "qwiklabs-gcp-04-db5d757ff012",
            "BQ_DATASET_ID": "Lab5",
            "BQ_TABLE_ID_AIRPORTS": "weather-data",
            "BQ_TABLE_ID_ALERTS": "weather_alerts",
            "API_KEY": "XXXXXXXXXXXXXXXXX"
        },
    )

    # Optional: Task to validate the data in BigQuery after the script runs
    validate_data_in_bigquery = BigQueryExecuteQueryOperator(
        task_id="validate_data_in_bigquery",
        sql=f"""
            SELECT COUNT(*) FROM `{dag.vars["PROJECT_ID"]}.{dag.vars["BQ_DATASET_ID"]}.{dag.vars["BQ_TABLE_ID_ALERTS"]}`
        """,
        use_legacy_sql=False,
    )

    # Define the task sequence
    run_weather_script >> validate_data_in_bigquery

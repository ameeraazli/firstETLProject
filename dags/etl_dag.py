from csv_to_postgres import update_pg, run_aggregate_queries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

def update_pg_wrapper():
  update_pg()

def run_aggregate_queries_wrapper():
  run_aggregate_queries()

with DAG(
  dag_id="etl_dag",
  schedule ="@hourly",
  default_args={
    "owner": "admin",
    "retries": 1,
    "retry": timedelta(minutes=5),
    "start_date": datetime(2023, 8, 29),
  },
  catchup=False) as dag:

  task_1 = PythonOperator(
    task_id="update_pg",
    python_callable=update_pg_wrapper
  )

  task_2 = PythonOperator(
    task_id="run_aggregate_queries",
    python_callable=run_aggregate_queries_wrapper
  )

  task_1 >> task_2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

import pandas as pd

def _extract_data_from_url(**kwargs):
  import requests
  url = "https://raw.githubusercontent.com/datasets/london-crime/master/data/crime-rates.csv"
  response = requests.get(url)
  response.raise_for_status()

  df = pd.read_csv(url)

  print(df)
  ti = kwargs["ti"]
  ti.xcom_push("df", df)

def create_connection(**kwargs):
  import psycopg2
  connection = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=5432
  )
  ti = kwargs["ti"]
  ti.xcom_push("connection", connection)

def _update_postgres_table(**kwargs):
  ti = kwargs["ti"]
  
  extract_connection = ti.xcom_pull(task_ids="update_postgres_table", key="connection")
  connection = json.loads(extract_connection)
  
  cursor = connection.cursor()

  extract_df = ti.xcom_pull(task_ids="update_postgres_table", key="df")
  df = json.loads(extract_df)

  for index, row in df.iterrows():
    query = f"UPSERT INTO crime_date (code, borough, crimeyear, value) VALUES ('{row['Code']}', '{row['Borough']}', '{row['Year']}', '{row['Value']}')"
    cursor.execute(query)

  connection.commit()
  cursor.close()

  print("successful insertion into database")
  connection.close()

def _run_aggregate_queries(**kwargs):
  ti = kwargs["ti"]
  
  extract_connection = ti.xcom_pull(task_ids="run_aggregate_queries", key="connection")
  connection = json.loads(extract_connection)

  cursor = connection.cursor()

  cursor.execute("SELECT MIN(value) FROM crime_date WHERE value <> 'NaN'")
  min_crime_date = cursor.fetchone()
  print("The minimum crime rate in all of UK's borough:" + str(min_crime_date[0]))

  cursor.execute("SELECT MAX(value) FROM crime_date WHERE value <> 'NaN'")
  max_crime_date = cursor.fetchone()
  print("The maximum crime rate in all of UK's borough:" + str(max_crime_date[0]))

  cursor.execute("SELECT AVG(value) FROM crime_date WHERE value <> 'NaN'")
  avg_crime_date = cursor.fetchone()
  print("The average crime rate in all of UK's borough:" + str(avg_crime_date[0]))

  cursor.execute("SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) FROM crime_date WHERE value <> 'NaN'")
  median_crime_date = cursor.fetchone()
  print("The median crime rate in UK's borough:" + str(median_crime_date[0]))

  cursor.close()

  print("successful aggregate operations")
  connection.close()

with DAG(
  dag_id="etl_dag",
  schedule ="@daily",
  default_args={
    "owner": "admin",
    "retries": 1,
    "retry": timedelta(minutes=5),
    "start_date": datetime(2023, 8, 29),
  },
  catchup=False) as dag:

  data_extraction_task = PythonOperator(
    task_id="extract_data_from_url",
    python_callable=_extract_data_from_url
  )

  psql_insertion_task = PythonOperator(
    task_id="update_postgres_table",
    python_callable=_update_postgres_table
  )

  aggregate_operations_task = PythonOperator(
    task_id="run_aggregate_queries",
    python_callable=_run_aggregate_queries
  )

  data_extraction_task >> psql_insertion_task >> aggregate_operations_task

  

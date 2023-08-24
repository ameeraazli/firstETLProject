import requests
import pandas as pd
import psycopg2

DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "helloworld123"

def extract_data_from_url(url):
  response = requests.get(url)
  response.raise_for_status()

  df = pd.read_csv(url)

  return df

def update_postgres_table(df, connection):
  cursor = connection.cursor()

  for index, row in df.iterrows():
    query = f"INSERT INTO crime_rate (code, borough, crimeyear, value) VALUES ('{row['Code']}', '{row['Borough']}', '{row['Year']}', '{row['Value']}')"
    cursor.execute(query)

  connection.commit()
  cursor.close()

  print("successful insertion into database")

def run_aggregate_queries(df, connection):
  cursor = connection.cursor()
  cursor.execute("SELECT MIN(value) FROM crime_rate WHERE value IS NOT NULL AND value <> 'NaN'")
  min_crime_rate = cursor.fetchone()
  print("The minimum crime rate in all of UK's borough:" + str(min_crime_rate[0]))

  cursor.execute("SELECT MAX(value) FROM crime_rate WHERE value IS NOT NULL AND value <> 'NaN'")
  max_crime_rate = cursor.fetchone()
  print("The maximum crime rate in all of UK's borough:" + str(max_crime_rate[0]))

  cursor.execute("SELECT AVG(value) FROM crime_rate WHERE value IS NOT NULL AND value <> 'NaN'")
  avg_crime_rate = cursor.fetchone()
  print("The average crime rate in all of UK's borough:" + str(avg_crime_rate[0]))

  cursor.execute("SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) FROM crime_rate WHERE value IS NOT NULL AND value <> 'NaN'")
  median_crime_rate = cursor.fetchone()
  print("The median crime rate in UK's borough:" + str(median_crime_rate[0]))

  cursor.close()

if __name__ == "__main__":
  csv_url = "https://raw.githubusercontent.com/datasets/london-crime/master/data/crime-rates.csv"
  csv_data = extract_data_from_url(csv_url)
  print(csv_data)

  connection = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port= 5432
  )

  update_postgres_table(csv_data, connection)

  run_aggregate_queries(csv_data, connection)

  connection.close()

  print("Connection closed")



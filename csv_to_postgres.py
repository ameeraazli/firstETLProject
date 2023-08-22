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

def update_postgres_table(df):
  connection = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
  )

  cursor = connection.cursor()

  for index, row in df.iterrows():
    query = f"INSERT INTO crime_rate (code, borough, crimeyear, value) VALUES ('{row['Code']}', '{row['Borough']}', '{row['Year']}', '{row['Value']}')"
    cursor.execute(query)

  connection.commit()
  cursor.close()
  connection.close()

  print("success")

if __name__ == "__main__":
  csv_url = "https://raw.githubusercontent.com/datasets/london-crime/master/data/crime-rates.csv"
  csv_data = extract_data_from_url(csv_url)
  print(csv_data)

  update_postgres_table(csv_data)



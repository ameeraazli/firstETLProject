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

  # return df
  return response.text

# def update_postgres_table(df):
#   connection = psycopg2.connect(
#     host=DB_HOST,
#     database=DB_NAME,
#     user=DB_USER,
#     password=DB_PASSWORD
#   )

#   cursor = connection.cursor()

#   for index, row in df.iterrows():
#     query = """
#             INSERT INTO OnlineRetail (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerId, Country)
#             VALUES('{row['InvoiceNo']}', '{row['StockCode']}', '{row['Description']}', '{row['Quantity']}', '{row['InvoiceDate']}', '{row['UnitPrice']}',
#             '{row['CustomerId']}', '{row['Country']}')
#             """

#   connection.commit()
#   cursor.close()
#   connection.close()

if __name__ == "__main__":
  csv_url = "https://raw.githubusercontent.com/datasets/house-prices-uk/main/data/data.csv"
  csv_data = extract_data_from_url(csv_url)

  # update_postgres_table(csv_data)


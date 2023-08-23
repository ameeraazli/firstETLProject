import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Create a sample DataFrame
data = {
    'id': [1, 2, 3, 4],
    'name': ['Alice', 'Bob', 'Charlie', 'David'],
    'age': [25, 30, 22, 40],
    'salary': [60000, 55000, 70000, 80000]
}
df = pd.DataFrame(data)

# Convert DataFrame to Arrow Table
table = pa.Table.from_pandas(df)

# Specify the Parquet file path
parquet_file_path = 'sample_data.parquet'

# Write Arrow Table to Parquet file
pq.write_table(table, parquet_file_path)

print("Sample columnar dataset saved in Parquet format.")
print(table)

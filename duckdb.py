import duckdb
import pandas as pd

conn = duckdb.connect()

df = conn.execute("""
    SELECT * FROM read_csv_auto('/Users/rohithb/Desktop/new_psark/orders.csv')
    LIMIT 12
""").df()

print(df)
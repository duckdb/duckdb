import pandas as pd
import duckdb

df = pd.DataFrame([{"join_column":"text"}])

try:
    duckdb_conn = duckdb.connect()
    duckdb_conn.register('main_table',df)
    cur = duckdb_conn.cursor()
    output_df = duckdb_conn.execute("select main_table.* from main_table").fetchdf()
except Exception as err:
    print(err)
finally:
    duckdb_conn.close()
    
print(output_df)
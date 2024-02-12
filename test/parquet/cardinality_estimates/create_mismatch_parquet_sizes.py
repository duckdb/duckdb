import duckdb

con = duckdb.connect('delete_me.duckdb')

con.sql(
    f"CREATE table all_data as select range a, range*2 b, range::VARCHAR c, range::VARCHAR || \'sdkfjssdfs\' d from range(0,0)"
)

row_num = 0
for i in range(2):
    extra_rows = 0
    con.sql(
        f"CREATE OR REPLACE table table_{i} as select range a, range*2 b, range::VARCHAR c, range::VARCHAR || \'sdkfjssdfs\' d from range({row_num}, {row_num+extra_rows})"
    )
    con.sql(
        f"INSERT INTO all_data (select range a, range*2 b, range::VARCHAR c, range::VARCHAR || \'sdkfjssdfs\' d from range({row_num}, {row_num+extra_rows}))"
    )
    con.sql(f"copy table_{i} to 'table_{i}.parquet' (FORMAT PARQUET)")
    con.sql(f"DROP TABLE table_{i}")
    row_num += extra_rows

for i in range(2, 4):
    extra_rows = 10000000
    con.sql(
        f"CREATE OR REPLACE table table_{i} as select range a, range*2 b, range::VARCHAR c, range::VARCHAR || \'sdkfjssdfs\' d from range({row_num}, {row_num+extra_rows})"
    )
    con.sql(
        f"INSERT INTO all_data (select range a, range*2 b, range::VARCHAR c, range::VARCHAR || \'sdkfjssdfs\' d from range({row_num}, {row_num+extra_rows}))"
    )
    con.sql(f"copy table_{i} to 'table_{i}.parquet' (FORMAT PARQUET)")
    con.sql(f"DROP TABLE table_{i}")
    row_num += extra_rows

con.sql("COPY all_data to 'all_data.parquet' (FORMAT PARQUET)")

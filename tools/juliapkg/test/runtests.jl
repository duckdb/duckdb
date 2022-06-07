import DuckDB
con = DuckDB.connect(":memory:")

res = DuckDB.execute(con,"CREATE TABLE interval(interval INTERVAL);")
res = DuckDB.execute(con,"""
INSERT INTO interval VALUES 
(INTERVAL 5 HOUR),
(INTERVAL 12 MONTH),
(INTERVAL 12 MICROSECOND),
(INTERVAL 1 YEAR);
""")
res = DuckDB.toDataFrame(con,"SELECT * FROM interval;")

res = DuckDB.execute(con,"CREATE TABLE timestamp(timestamp TIMESTAMP , data INTEGER);")
res = DuckDB.execute(con,"INSERT INTO timestamp VALUES ('2021-09-27 11:30:00.000', 4), ('2021-09-28 12:30:00.000', 6), ('2021-09-29 13:30:00.000', 8);")
res = DuckDB.execute(con,"SELECT * FROM timestamp;")
res = DuckDB.toDataFrame(res)

res = DuckDB.execute(con, "CREATE TABLE items(item VARCHAR, value DECIMAL(10,2), count INTEGER);")
res = DuckDB.execute(con, "INSERT INTO items VALUES ('jeans', 20.0, 1), ('hammer', 42.2, 2);")
res = DuckDB.toDataFrame(con, "SELECT * FROM items;")

res = DuckDB.execute(con,"CREATE TABLE integers(date DATE, data INTEGER);")
res = DuckDB.execute(con,"INSERT INTO integers VALUES ('2021-09-27', 4), ('2021-09-28', 6), ('2021-09-29', 8);")
res = DuckDB.execute(con,"SELECT * FROM integers;")
res = DuckDB.toDataFrame(res)

DuckDB.disconnect(con)
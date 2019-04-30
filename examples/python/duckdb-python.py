import duckdb

# connect to an in-memory temporary database
cursor = duckdb.connect('').cursor()

# showcase the various ways of fetching query results

# fetch as pandas data frame
print(cursor.execute("SELECT 1 AS a, 'DPFKG' AS b UNION ALL SELECT NULL, NULL").fetchdf())

# fetch as list of masked numpy arrays
print(cursor.execute("SELECT 1 AS a, 'DPFKG' AS b UNION ALL SELECT NULL, NULL").fetchnumpy())

# compatibility version to fetch python lists
print(cursor.execute("SELECT 1 AS a, 'DPFKG' AS b UNION ALL SELECT NULL, NULL").fetchall())


import pyodbc
import glob

cnxn = pyodbc.connect('DSN={DuckDB}')
cursor = cnxn.cursor()

cursor.execute("CREATE TABLE fuu (i INTEGER, j STRING)")
cursor.execute("INSERT INTO fuu VALUES (42, 'Hello'), (43, 'World'), (NULL, NULL)")

install.packages("odbc", repos="https://cloud.r-project.org/")

library("DBI")
con <- dbConnect(odbc::odbc(), database=":memory:", driver=Sys.glob("build/debug/tools/odbc/libduckdb_odbc.*"))

dbExecute(con, "CREATE TABLE fuu (i INTEGER, j STRING)")
dbExecute(con, "INSERT INTO fuu VALUES (42, 'Hello'), (43, 'World'), (NULL, NULL)")

print(dbListTables(con))
print(dbReadTable(con, "fuu"))

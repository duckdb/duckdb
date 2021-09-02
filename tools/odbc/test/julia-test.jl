import Pkg

Pkg.add("ODBC")
Pkg.add("DataFrames")

using ODBC,DataFrames

home = ENV["HOME"]
ODBC.setunixODBC(;ODBCSYSINI=home, ODBCINSTINI=".odbcinst.ini", ODBCINI=string(home, "/.odbc.ini"))

conn=ODBC.Connection("DSN=DuckDB;")

duckdb_dir = pwd()
DBInterface.execute(conn,"CREATE TABLE test AS SELECT * FROM read_csv_auto('" * duckdb_dir * "/tools/odbc/test/test.csv')")

df1 = DBInterface.execute(conn,"SELECT sum(a) as a, sum(b) as b, sum(c) as c FROM test")|>DataFrame
df2 = DataFrame(a = Float64(3.3), b = Float64(36.0), c = Float64(1.0));

if df1 != df2
    exit(1)
end

println("Julia successful \\o/")

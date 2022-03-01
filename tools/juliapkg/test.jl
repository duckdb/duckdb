include("src/DuckDB.jl");

using DBInterface
using Debugger
using DataFrames

db = DBInterface.connect(DuckDB.DB)


# print(db)

# error in connect
# db = DBInterface.connect("/path/to/bogus/directory")

# parser error
# res = DBInterface.execute(db, "SELEC")

# binder error
# res = DBInterface.execute(db, "SELECT * FROM this_table_does_not_exist")

# error in execute
# res = DBInterface.execute(db, "SELECT i::int FROM (SELECT '42' UNION ALL SELECT 'hello') tbl(i)")


# @enter res = DBInterface.execute(db, "SELECT 42")
# print(res)
# for row in res
# 	println(row);
#     @show propertynames(row) # see possible column names of row results
#     println(row.a) # access the value of a column named `col1`
#     println(row[1]) # access the first column in the row results
# end

res = DBInterface.execute(db, "SELECT 42::TINYINT a, 42::INT16 b, 42::INT32 c, 42::INT64 d, 42::UINT8 e, 42::UINT16 f, 42::UINT32 g, 42::UINT64 h, 'hello world' i, TRUE::BOOL k, DATE '1992-01-01' l")

df = DataFrame(res)
println(df)
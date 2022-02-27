include("src/DuckDB.jl");

using DBInterface
using Debugger

db = DBInterface.connect()


# print(db)

res = DBInterface.execute(db, "SELECT 42 a union all select 84")
# @enter res = DBInterface.execute(db, "SELECT 42")
# print(res)
for row in res
	println(row);
    @show propertynames(row) # see possible column names of row results
    println(row.a) # access the value of a column named `col1`
    println(row[1]) # access the first column in the row results
end

include("src/DuckDB.jl");

using DBInterface
using Debugger

db = DBInterface.connect()


print(db)

res = DBInterface.execute(db, "SELECT 42")
# @enter res = DBInterface.execute(db, "SELECT 42")
print(res)
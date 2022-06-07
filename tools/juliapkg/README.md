# Julia C-API wrapper for DuckDB
## Installation
pkg> add https://github.com/kimmolinna/DuckDB.jl

julia> import DuckDB

## Basics
con = DuckDB.connect(":memory:")<br>
res = DuckDB.execute(con,"CREATE TABLE integers(date DATE, jcol INTEGER)")<br>
res = DuckDB.execute(con,"INSERT INTO integers VALUES ('2021-09-27', 4), ('2021-09-28', 6), ('2021-09-29', 8)")<br>
res = DuckDB.execute(con, "SELECT * FROM integers")<br>
DuckDB.toDataFrame(res)<br>
DuckDB.disconnect(con)<br>

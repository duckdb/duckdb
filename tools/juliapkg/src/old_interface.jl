# old interface, deprecated
using DataFrames

open(dbpath::AbstractString) = DBInterface.connect(DuckDB.DB, dbpath)

connect(db::DB) = DBInterface.connect(db)

disconnect(con::Connection) = DBInterface.close!(con)
close(db::DB) = DBInterface.close!(db)

toDataFrame(con::Connection, sql::AbstractString) = DBInterface.execute(con, sql)

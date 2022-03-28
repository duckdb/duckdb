# old interface, deprecated
using DataFrames

open(dbpath::AbstractString) = DBInterface.connect(DuckDB.DB, dbpath)

connect(db::DB) = DBInterface.connect(db)

disconnect(con::Connection) = DBInterface.close!(con)
close(db::DB) = DBInterface.close!(db)

toDataFrame(res::QueryResult) = res.df
toDataFrame(con::Connection, sql::AbstractString) = toDataFrame(DBInterface.execute(con, sql))

function appendDataFrame(input_df::DataFrame, con::Connection, table::AbstractString, schema::String = "main")
    register_data_frame(con, input_df, "__append_df")
    DBInterface.execute(con, "INSERT INTO \"$schema\".\"$table\" SELECT * FROM __append_df")
    return unregister_data_frame(con, "__append_df")
end

appendDataFrame(input_df::DataFrame, db::DB, table::AbstractString, schema::String = "main") =
    appendDataFrame(input_df, db.main_connection, table, schema)

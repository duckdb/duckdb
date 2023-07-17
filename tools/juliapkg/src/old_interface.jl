# old interface, deprecated
open(dbpath::AbstractString) = DBInterface.connect(DuckDB.DB, dbpath)

connect(db::DB) = DBInterface.connect(db)

disconnect(con::Connection) = DBInterface.close!(con)
close(db::DB) = DBInterface.close!(db)

# not really a dataframe anymore
# if needed for backwards compatibility, can add through Requires/1.9 extension
toDataFrame(r::QueryResult) = Tables.columntable(r)
toDataFrame(con::Connection, sql::AbstractString) = toDataFrame(DBInterface.execute(con, sql))

function appendDataFrame(input_df, con::Connection, table::AbstractString, schema::String = "main")
    register_data_frame(con, input_df, "__append_df")
    DBInterface.execute(con, "INSERT INTO \"$schema\".\"$table\" SELECT * FROM __append_df")
    return unregister_data_frame(con, "__append_df")
end

appendDataFrame(input_df, db::DB, table::AbstractString, schema::String = "main") =
    appendDataFrame(input_df, db.main_connection, table, schema)

"""
    DuckDB.load!(con, input_df, table)

Load an input DataFrame `input_df` into a new DuckDB table that will be named `table`.
"""
function load!(con, input_df, table::AbstractString, schema::String = "main")
    register_data_frame(con, input_df, "__append_df")
    DBInterface.execute(con, "CREATE TABLE \"$schema\".\"$table\" AS SELECT * FROM __append_df")
    unregister_data_frame(con, "__append_df")
    return
end

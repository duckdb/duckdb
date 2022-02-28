
#
# #Macros
# macro OK(func)
#     :($(esc(func)) == 0)
# end
#
# macro CHECK(db,ex)
#     esc(quote
#         if !(@OK $ex)
#             #sqliteerror($db)
#         end
#         SQLITE_OK
#     end)
# end
#
#
# """
# Internal wrapper that holds the handle to SQLite3 prepared statement.
# It is managed by [`SQLite.DB`](@ref) and referenced by the "public" [`SQLite.Stmt`](@ref) object.
#
# When no `SQLite.Stmt` instances reference the given `SQlite._Stmt` object,
# it is closed automatically.
#
# When `SQLite.DB` is closed or [`SQLite.finalize_statements!`](@ref) is called,
# all its `SQLite._Stmt` objects are closed.
# """
# mutable struct _Stmt
#     handle::duckdb_prepared_statement
#     params::Dict{Int, Any}
#
#     function _Stmt(handle::duckdb_prepared_statement)
#         stmt = new(handle, Dict{Int, Any}())
#         finalizer(_close!, stmt)
#         return stmt
#     end
# end
#
# # close statement
# function _close!(stmt::_Stmt)
#     stmt.handle == C_NULL || sqlite3_finalize(stmt.handle)
#     stmt.handle = C_NULL
#     return
# end
#
# # _Stmt unique identifier in DB
# const _StmtId = Int
#
mutable struct DuckDBHandle
    file::String
    handle::duckdb_database

    function DuckDBHandle(f::AbstractString)
        f = String(isempty(f) ? f : expanduser(f))
        handle = Ref{duckdb_database}()
        error = Ref{Ptr{UInt8}}()
        if duckdb_open_ext(f, handle, C_NULL, error) != DuckDBSuccess
            error_message = unsafe_string(error[])
            duckdb_free(error[])
            throw(ConnectionException(error_message))
        end

        db = new(f, handle[])
        finalizer(_close_database, db)
        return db
    end
end

function _close_database(db::DuckDBHandle)
    # disconnect from DB
    if db.handle != C_NULL
        duckdb_close(db.handle)
    end
    db.handle = C_NULL
    return
end

mutable struct Connection
    db::DuckDBHandle
    handle::duckdb_connection

    function Connection(db::DuckDBHandle)
        handle = Ref{duckdb_connection}()
        if duckdb_connect(db.handle, handle) != DuckDBSuccess
            throw(ConnectionException("Failed to open connection"))
        end
        con = new(db, handle[])
        finalizer(_close_connection, con)
        return con
    end
end

function _close_connection(con::Connection)
    # disconnect
    if con.handle != C_NULL
        duckdb_disconnect(con.handle)
    end
    con.handle = C_NULL
    return
end

mutable struct DB <: DBInterface.Connection
    handle::DuckDBHandle
    main_connection::Connection

    function DB(f::AbstractString)
        handle = DuckDBHandle(f)
        main_connection = Connection(handle)
        db = new(handle, main_connection)
        return db
    end
end

function close_database(db::DB)
    _close_connection(db.main_connection)
    return _close_database(db.handle)
end

DB() = DB(":memory:")
DBInterface.connect() = DB()
DBInterface.connect(f::AbstractString) = DB(f)
DBInterface.close!(db::DB) = close_database(db)
Base.close(db::DB) = close_database(db)
Base.isopen(db::DB) = db.handle != C_NULL

#
# sqliteerror(db::DB) = sqliteerror(db.handle)
# sqliteexception(db::DB) = sqliteexception(db.handle)
#
# Base.show(io::IO, db::DuckDB.DB) = print(io, string("DuckDB.DB(", "\"$(db.file)\"", ")"))
#

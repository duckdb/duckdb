"""
Internal DuckDB database handle.
"""
mutable struct DuckDBHandle
    file::String
    handle::duckdb_database
    functions::Vector{Any}
    registered_objects::Dict{Any, Any}

    function DuckDBHandle(f::AbstractString, config::Config)
        f = String(isempty(f) ? f : expanduser(f))
        handle = Ref{duckdb_database}()
        error = Ref{Ptr{UInt8}}()
        if duckdb_open_ext(f, handle, config.handle, error) != DuckDBSuccess
            error_message = unsafe_string(error[])
            duckdb_free(error[])
            throw(ConnectionException(error_message))
        end

        db = new(f, handle[], Vector(), Dict())
        finalizer(_close_database, db)
        return db
    end
end

function _close_database(db::DuckDBHandle)
    # disconnect from DB
    if db.handle != C_NULL
        duckdb_close(db.handle)
    end
    return db.handle = C_NULL
end

"""
A connection object to a DuckDB database.

Transaction contexts are local to a single connection.

A connection can only run a single query concurrently.
It is possible to open multiple connections to a single DuckDB database instance.
Multiple connections can run multiple queries concurrently.
"""
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

"""
A DuckDB database object.

By default a DuckDB database object has an open connection object (db.main_connection).
When the database object is used directly in queries, it is actually the underlying main_connection that is used.

It is possible to open new connections to a single database instance using DBInterface.connect(db).
"""
mutable struct DB <: DBInterface.Connection
    handle::DuckDBHandle
    main_connection::Connection

    function DB(f::AbstractString, config::Config)
        set_config(config, "threads", "1")
        set_config(config, "external_threads", string(Threads.nthreads() - 1))
        handle = DuckDBHandle(f, config)
        main_connection = Connection(handle)

        db = new(handle, main_connection)
        _add_data_frame_scan(db)
        return db
    end
    function DB(f::AbstractString)
        return DB(f, Config())
    end
end

function close_database(db::DB)
    _close_connection(db.main_connection)
    _close_database(db.handle)
    return
end

const VECTOR_SIZE = duckdb_vector_size()
const ROW_GROUP_SIZE = VECTOR_SIZE * 100

DB() = DB(":memory:")
DBInterface.connect(::Type{DB}) = DB()
DBInterface.connect(::Type{DB}, f::AbstractString) = DB(f)
DBInterface.connect(::Type{DB}, f::AbstractString, config::Config) = DB(f, config)
DBInterface.connect(db::DB) = Connection(db.handle)
DBInterface.close!(db::DB) = close_database(db)
DBInterface.close!(con::Connection) = _close_connection(con)
Base.close(db::DB) = close_database(db)
Base.isopen(db::DB) = db.handle != C_NULL

Base.show(io::IO, db::DuckDB.DB) = print(io, string("DuckDB.DB(", "\"$(db.handle.file)\"", ")"))
Base.show(io::IO, con::DuckDB.Connection) = print(io, string("DuckDB.Connection(", "\"$(con.db.file)\"", ")"))

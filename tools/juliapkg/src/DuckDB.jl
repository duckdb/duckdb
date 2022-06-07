module DuckDB
using DataFrames, Dates, DuckDB_jll
using DBInterface, Tables

export DBInterface

include("api.jl")
include("consts.jl")

const DBHandle = Ref{Ptr{Cvoid}}   # DuckDB DB connection handle

"""
    toDataFrame(connection::Ref{Ptr{Cvoid}},query::String)::DataFrame

Creates a DataFrame from a SQL query within a connection.

* `connection`: The connection to perform the query in.
* `query`: The SQL query to run.
* returns: the abstract dataframe

"""
function toDataFrame(connection::Ref{Ptr{Cvoid}}, query::String)::DataFrame
    res = execute(connection, query)::Ref{duckdb_result}
    return toDataFrame(res)
end
"""
    toDataFrame(result::Ref{duckdb_result})::DataFrame

Creates a DataFrame from the full result
* `result`: the full result from `execute`
* returns: the abstract dataframe

"""
function toDataFrame(result::Ref{duckdb_result})::DataFrame
    columns =
        unsafe_wrap(Array{duckdb_column}, result[].columns, Int64(result[].column_count))
    df = DataFrame()
    for i = 1:Int64(result[].column_count)
        rows = Int64(result[].row_count)
        name = (unsafe_string(columns[i].name))
        type = DUCKDB_TYPE(Int64(columns[i].type))
        if type == DUCKDB_TYPE_INVALID
            print("invalid type for column - \"" * name * "\"")
        else
            mask = unsafe_wrap(Array, columns[i].nullmask, rows)
            data = unsafe_wrap(Array, Ptr{DUCKDB_TYPES[type]}(columns[i].data), rows)
            bmask = reinterpret(Bool, mask)

            if 0 != sum(mask)
                data = data[.!bmask]
            end
            if type == DUCKDB_TYPE_DATE
                data = Dates.epochdays2date.(data .+ 719528)
            elseif type == DUCKDB_TYPE_TIME
                data = Dates.Time.(Dates.Nanosecond.(data .* 1000))
            elseif type == DUCKDB_TYPE_TIMESTAMP
                data = Dates.epochms2datetime.((data ./ 1000) .+ 62167219200000)
            elseif type == DUCKDB_TYPE_INTERVAL
                data = map(
                    x -> Dates.CompoundPeriod(
                        Dates.Month(x.months),
                        Dates.Day(x.days),
                        Dates.Microsecond(x.micros),
                    ),
                    data,
                )
            elseif type == DUCKDB_TYPE_HUGEINT
                data = map(x -> x.upper < 1 ? (x.lower::UInt64)%Int64 : x, data)
            elseif type == DUCKDB_TYPE_VARCHAR
                data = unsafe_string.(data)
            end


            if 0 != sum(mask)
                fulldata = Array{Union{Missing,eltype(data)}}(missing, rows)
                fulldata[.!bmask] = data
                data = fulldata
            end

            df[!, name] = data
        end
    end
    return df
end
"""
    disconnect(connection)
Closes the specified connection and de-allocates all memory allocated for that connection.
* `connection`: The connection to close.

"""
function disconnect(connection)
    return duckdb_disconnect(connection)
end

"""
    close(database)
Closes the specified database and de-allocates all memory allocated for that database.\n
This should be called after you are done with any database allocated through duckdb_open.\n
Note that failing to call duckdb_close (in case of e.g. a program crash) will not cause data corruption. Still it is recommended to always correctly close a database object after you are done with it.
*`database`: the database object to shut down.

"""
function close(database)
    return duckdb_close(database)
end

connect() = connect(":memory:")

"""
    connect(path)
Creates a new database or opens an existing database file stored at the the given path. If no path is given a new in-memory database is created instead.
* `path`: Path to the database file on disk or `:memory:` to open an in-memory database.
* returns: a connection handle

"""
function connect(path::String)::Ref{Ptr{Cvoid}}
    database = Ref{Ptr{Cvoid}}()
    connection = Ref{Ptr{Cvoid}}()
    duckdb_open(path, database)
    duckdb_connect(database, connection)
    return connection
end

"""
    execute(connection, query)

Executes a SQL query within a connection and returns the full (materialized) result. If the query fails to execute, `DuckDBError` is returned and the error message can be retrieved by calling `duckdb_result_error`.

Note that after running duckdb_query, duckdb_destroy_result must be called on the result object even if the query fails, otherwise the error stored within the result will not be freed correctly.
* `connection`: The connection to perform the query in.
* `query`: The SQL query to run.
* returns: the full result pointer

"""
function execute(connection::Ref{Ptr{Cvoid}}, query::String)::Ref{duckdb_result}
    result = Ref{duckdb_result}()
    duckdb_query(connection, query, result)
    if result[].error_message != Ptr{UInt8}(0)
        print(unsafe_string(result[].error_message))
    end
    return result
end

struct DuckDBException <: Exception
    msg::AbstractString
end

duckdb_errmsg(handle) = unsafe_string(handle[].error_message)
duckdbexception(handle::DBHandle) = DuckDBException(duckdb_errmsg(handle))
duckdberror(handle::DBHandle) = throw(duckdbexception(handle))

"""
    `DuckDB.DB()` => in-memory DuckDB database
    `DuckDB.DB(file)` => file-based DuckDB database

Constructors for a representation of a DuckDB database, either backed by an on-disk file or in-memory.

`DuckDB.DB` requires the `file` string argument in the 2nd definition
as the name of either a pre-defined DuckDB database to be opened,
or if the file doesn't exist, a database will be created.

The `DuckDB.DB` object represents a single connection to a DuckDB database.
All other DuckDB.jl functions take an `DuckDB.DB` as the first argument as context.

To create an in-memory temporary database, call `DuckDB.DB()`.

The `DuckDB.DB` will be automatically closed/shutdown when it goes out of scope
(i.e. the end of the Julia session, end of a function call wherein it was created, etc.)

NOTE: This borrows heavily from SQLite.jl, [here](https://github.com/JuliaDatabases/SQLite.jl/blob/9724a175ae30b22cb01775250ce2e87317706215/src/SQLite.jl)
"""
mutable struct DB <: DBInterface.Connection
    file::String
    handle::DBHandle

    # TODO: Support prepared statements

    function DB(f::AbstractString)
        f = String(isempty(f) ? f : expanduser(f))

        try
            handle = connect(f)

            db = new(f, handle)
            finalizer(_close, db)
            return db
        catch
            duckdberror(handle)
        end
    end
end

function _close(db::DB)
    db.handle == C_NULL || duckdb_disconnect(db.handle)
    db.handle = C_NULL
    return
end

DB() = DB(":memory:")
DBInterface.close!(db::DB) = _close(db)
Base.close(db::DB) = _close(db)

Base.show(io::IO, db::DB) = print(io, string("DB(", "\"$(db.file)\"", ")"))

"""
    DBInterface.execute(db::DB, sql::String, [params])

TODO: Support DBInterface statements

Take inputs given by `db` and `sql` execute the query and return an iterator of result rows.

Note that the returned result row iterator only supports a single-pass, forward-only iteration of the result rows.

TODO: Support Tables.jl
"""
function DBInterface.execute(db::DB, sql::String)

    result = execute(db.handle, sql)

    return result
end

end # module

module DuckDB
using DataFrames,Dates,DuckDB_jll
include("api.jl")
include("consts.jl")

"""
    toDataFrame(result)
Creates a DataFrame from the full result
* `result`: the full result from `execute`
* returns: the abstract dataframe

"""
function toDataFrame(result)
    columns=unsafe_wrap(Array{duckdb_column},result[].columns,Int64(result[].column_count));
    df = DataFrame();
    for i in 1:Int64(result[].column_count)
        rows = Int64(result[].row_count)
        name = (unsafe_string(columns[i].name))
        type = DUCKDB_TYPE(Int64(columns[i].type))
        if type == DUCKDB_TYPE_INVALID
            print("invalid type for column - \""*name*"\"") 
        else
            mask = unsafe_wrap(Array,columns[i].nullmask,rows)
            data = unsafe_wrap(Array,Ptr{DUCKDB_TYPES[type]}(columns[i].data),rows)
            bmask=reinterpret(Bool,mask)

            if 0!=sum(mask)
                data = data[.!bmask]
            end
            if type == DUCKDB_TYPE_DATE
                data = Dates.epochdays2date.(data.+719528)
            elseif type == DUCKDB_TYPE_TIME
                data = Dates.Time.(Dates.Nanosecond.(data.*1000))
            elseif type == DUCKDB_TYPE_TIMESTAMP
                data = Dates.epochms2datetime.((data./1000).+62167219200000)
            elseif type == DUCKDB_TYPE_INTERVAL
                data = map(x -> Dates.CompoundPeriod(Dates.Month(x.months),Dates.Day(x.days),Dates.Microsecond(x.micros)),data)
            elseif type == DUCKDB_TYPE_VARCHAR
                data = unsafe_string.(data)
            end   


            if 0!=sum(mask)           
                fulldata = Array{Union{Missing, eltype(data)}}(missing, rows)
                fulldata[.!bmask] = data
                data = fulldata
            end

            df[!,name] = data           
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
    closedb(database)
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
Creates a new database or opens an existing database file stored at the the given path. If no path is given a new in-memory database is created instead.
* path: Path to the database file on disk or :memory: to open an in-memory database.
* returns: a connection handle

"""
function connect(file)
    database = Ref{Ptr{Cvoid}}()
    connection = Ref{Ptr{Cvoid}}()
    duckdb_open(file,database)
    duckdb_connect(database,connection)
    return connection
end

"""
Executes a SQL query within a connection and returns the full (materialized) result. If the query fails to execute, DuckDBError is returned and the error message can be retrieved by calling duckdb_result_error.

Note that after running duckdb_query, duckdb_destroy_result must be called on the result object even if the query fails, otherwise the error stored within the result will not be freed correctly.
* connection: The connection to perform the query in.
* query: The SQL query to run.
* returns: the full result pointer

"""
function execute(connection,query) 
    result = Ref{duckdb_result}()
    duckdb_query(connection,query,result)
    if result[].error_message==Ptr{UInt8}(0)
        return result
    else
        return unsafe_string(result[].error_message)
    end
end

end # module

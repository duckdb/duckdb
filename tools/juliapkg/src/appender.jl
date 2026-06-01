using Dates

"""
    Appender(db_connection, table, [schema])
    
An appender object that can be used to append rows to an existing table.

* DateTime objects in Julia are stored in milliseconds since the Unix epoch but are converted to microseconds when stored in duckdb.
* Time objects in Julia are stored in nanoseconds since midnight but are converted to microseconds when stored in duckdb.
* Missing and Nothing are stored as NULL in duckdb, but will be converted to Missing when the data is queried back.

# Example
```julia
using DuckDB, DataFrames, Dates
db = DuckDB.DB()

# create a table
DBInterface.execute(db, "CREATE OR REPLACE TABLE data(id INT PRIMARY KEY, value FLOAT, timestamp TIMESTAMP, date DATE)")

# data to insert 
len = 100
df = DataFrames.DataFrame(id=collect(1:len),
    value=rand(len),
    timestamp=Dates.now() + Dates.Second.(1:len),
    date=Dates.today() + Dates.Day.(1:len))

# append data by row
appender = DuckDB.Appender(db, "data")
for i in eachrow(df)
    for j in i
        DuckDB.append(appender, j)
    end
    DuckDB.end_row(appender)
end
# flush the appender after all rows
DuckDB.flush(appender)
DuckDB.close(appender)
```
"""
mutable struct Appender
    connection::Connection
    handle::duckdb_appender

    function Appender(con::Connection, table::AbstractString, schema::Union{AbstractString, Nothing} = nothing)
        handle = Ref{duckdb_appender}()
        if duckdb_appender_create(con.handle, something(schema, C_NULL), table, handle) != DuckDBSuccess
            error_ptr = duckdb_appender_error(handle)
            error_s = "unknown error"
            if error_ptr != C_NULL
                error_s = unsafe_string(error_ptr)
            end
            error_message = "Opening of Appender for table \"$table\" failed: $error_s"
            duckdb_appender_destroy(handle)
            throw(QueryException(error_message))
        end
        app = new(con, handle[])
        finalizer(_close_appender, app)
        return app
    end

    function Appender(db::DB, table::AbstractString, schema::Union{AbstractString, Nothing} = nothing)
        return Appender(db.main_connection, table, schema)
    end
end

function _close_appender(appender::Appender)
    res = DuckDBSuccess
    if appender.handle != C_NULL
        # After duckdb_appender_destroy() is called any error message can't be retrieved
        # anymore with duckdb_appender_error(). So ..._close() it first, check for error,
        # and retrieve the error message if needed, before calling ..._destroy(). We could
        # return the error in case it occurs, but raising an exception here matches other
        # calls of Appender.
        res = duckdb_appender_close(appender.handle)
        if res != DuckDBSuccess
            error_ptr = duckdb_appender_error(appender.handle)
            error_s = "unknown error"
            if error_ptr != C_NULL
                error_s = unsafe_string(error_ptr)
            end
            error_message = "Closing of Appender failed: $error_s"
            duckdb_appender_destroy(handle)
            throw(ConnectionException(error_message))
        end
        duckdb_appender_destroy(appender.handle)
    end
    appender.handle = C_NULL
    return res
end

function close(appender::Appender)
    return _close_appender(appender)
end

append(appender::Appender, val::AbstractFloat) = duckdb_append_double(appender.handle, Float64(val));
append(appender::Appender, val::Bool) = duckdb_append_bool(appender.handle, val);
append(appender::Appender, val::Int8) = duckdb_append_int8(appender.handle, val);
append(appender::Appender, val::Int16) = duckdb_append_int16(appender.handle, val);
append(appender::Appender, val::Int32) = duckdb_append_int32(appender.handle, val);
append(appender::Appender, val::Int64) = duckdb_append_int64(appender.handle, val);
append(appender::Appender, val::Int128) = duckdb_append_hugeint(appender.handle, val);
append(appender::Appender, val::UInt128) = duckdb_append_uhugeint(appender.handle, val);
append(appender::Appender, val::UInt8) = duckdb_append_uint8(appender.handle, val);
append(appender::Appender, val::UInt16) = duckdb_append_uint16(appender.handle, val);
append(appender::Appender, val::UInt32) = duckdb_append_uint32(appender.handle, val);
append(appender::Appender, val::UInt64) = duckdb_append_uint64(appender.handle, val);
append(appender::Appender, val::Float32) = duckdb_append_float(appender.handle, val);
append(appender::Appender, val::Float64) = duckdb_append_double(appender.handle, val);
append(appender::Appender, ::Union{Missing, Nothing}) = duckdb_append_null(appender.handle);
append(appender::Appender, val::AbstractString) = duckdb_append_varchar(appender.handle, val);
append(appender::Appender, val::Base.UUID) = append(appender, string(val));
append(appender::Appender, val::Vector{UInt8}) = duckdb_append_blob(appender.handle, val, sizeof(val));
append(appender::Appender, val::FixedDecimal) = append(appender, string(val));
# append(appender::Appender, val::WeakRefString{UInt8}) = duckdb_append_varchar(stmt.handle, i, val.ptr, val.len);
append(appender::Appender, val::Date) =
    duckdb_append_date(appender.handle, Dates.date2epochdays(val) - ROUNDING_EPOCH_TO_UNIX_EPOCH_DAYS);
# nanosecond to microseconds
append(appender::Appender, val::Time) = duckdb_append_time(appender.handle, Dates.value(val) ÷ 1000);

# milliseconds to microseconds
append(appender::Appender, val::DateTime) =
    duckdb_append_timestamp(appender.handle, (Dates.datetime2epochms(val) - ROUNDING_EPOCH_TO_UNIX_EPOCH_MS) * 1000);

function append(appender::Appender, val::AbstractVector{T}) where {T}
    value = create_value(val)
    if length(val) == 0
        return duckdb_append_null(appender.handle)
    else
        return duckdb_append_value(appender.handle, value.handle)
    end
end

function append(appender::Appender, val::Any)
    throw(NotImplementedException("unsupported type for append: $(typeof(val))"))
end

function end_row(appender::Appender)
    return duckdb_appender_end_row(appender.handle)
end

function flush(appender::Appender)
    return duckdb_appender_flush(appender.handle)
end

DBInterface.close!(appender::Appender) = _close_appender(appender)

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
    handle::duckdb_appender

    function Appender(con::Connection, table::AbstractString, schema::Union{AbstractString, Nothing} = nothing)
        handle = Ref{duckdb_appender}()
        if duckdb_appender_create(con.handle, something(schema, C_NULL), table, handle) != DuckDBSuccess
            error_ptr = duckdb_appender_error(handle)
            if error_ptr == C_NULL
                error_message = string("Opening of Appender for table \"", table, "\" failed: unknown error")
            else
                error_message = string(error_ptr)
            end
            duckdb_appender_destroy(handle)
            throw(QueryException(error_message))
        end
        con = new(handle[])
        finalizer(_close_appender, con)
        return con
    end
    function Appender(db::DB, table::AbstractString, schema::Union{AbstractString, Nothing} = nothing)
        return Appender(db.main_connection, table, schema)
    end
end

function _close_appender(appender::Appender)
    if appender.handle != C_NULL
        duckdb_appender_destroy(appender.handle)
    end
    appender.handle = C_NULL
    return
end

function close(appender::Appender)
    _close_appender(appender)
    return
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
append(appender::Appender, val::Time) = duckdb_append_time(appender.handle, Dates.value(val) / 1000);
# milliseconds to microseconds
append(appender::Appender, val::DateTime) =
    duckdb_append_timestamp(appender.handle, (Dates.datetime2epochms(val) - ROUNDING_EPOCH_TO_UNIX_EPOCH_MS) * 1000);

function append(appender::Appender, val::Any)
    println(val)
    throw(NotImplementedException("unsupported type for append"))
end

function end_row(appender::Appender)
    duckdb_appender_end_row(appender.handle)
    return
end

function flush(appender::Appender)
    duckdb_appender_flush(appender.handle)
    return
end

DBInterface.close!(appender::Appender) = _close_appender(appender)

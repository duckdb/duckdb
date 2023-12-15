
"""
An appender object that can be used to append to a table
"""
mutable struct Appender
    handle::duckdb_appender

    function Appender(con::Connection, table::AbstractString)
        handle = Ref{duckdb_appender}()
        if duckdb_appender_create(con.handle, C_NULL, table, handle) != DuckDBSuccess
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
    function Appender(db::DB, table::AbstractString)
        return Appender(db.main_connection, table)
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
append(appender::Appender, val::Bool) = duckdb_append_boolean(appender.handle, val);
append(appender::Appender, val::Int8) = duckdb_append_int8(appender.handle, val);
append(appender::Appender, val::Int16) = duckdb_append_int16(appender.handle, val);
append(appender::Appender, val::Int32) = duckdb_append_int32(appender.handle, val);
append(appender::Appender, val::Int64) = duckdb_append_int64(appender.handle, val);
append(appender::Appender, val::UInt8) = duckdb_append_uint8(appender.handle, val);
append(appender::Appender, val::UInt16) = duckdb_append_uint16(appender.handle, val);
append(appender::Appender, val::UInt32) = duckdb_append_uint32(appender.handle, val);
append(appender::Appender, val::UInt64) = duckdb_append_uint64(appender.handle, val);
append(appender::Appender, val::Float32) = duckdb_append_float(appender.handle, val);
append(appender::Appender, val::Float64) = duckdb_append_double(appender.handle, val);
append(appender::Appender, val::Missing) = duckdb_append_null(appender.handle);
append(appender::Appender, val::Nothing) = duckdb_append_null(appender.handle);
append(appender::Appender, val::AbstractString) = duckdb_append_varchar(appender.handle, val);
append(appender::Appender, val::Vector{UInt8}) = duckdb_append_blob(appender.handle, val, sizeof(val));
append(appender::Appender, val::WeakRefString{UInt8}) = duckdb_append_varchar(stmt.handle, i, val.ptr, val.len);

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

DBInterface.close!(appender::Appender) = _close_appender(db)

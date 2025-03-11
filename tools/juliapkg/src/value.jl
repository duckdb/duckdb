"""
DuckDB value
"""
mutable struct Value
    handle::duckdb_value

    function Value(handle::duckdb_value)
        result = new(handle)
        finalizer(_destroy_value, result)
        return result
    end
end

function _destroy_value(val::Value)
    if val.handle != C_NULL
        duckdb_destroy_value(val.handle)
    end
    val.handle = C_NULL
    return
end

getvalue(val::Value, ::Type{T}) where {T <: Int64} = duckdb_get_int64(val.handle)
function getvalue(val::Value, ::Type{T}) where {T <: String}
    ptr = duckdb_get_varchar(val.handle)
    result = unsafe_string(ptr)
    duckdb_free(ptr)
    return result
end
function getvalue(val::Value, ::Type{T}) where {T}
    throw(NotImplementedException("Unsupported type for getvalue"))
end

create_value(val::T) where {T <: Bool} = Value(duckdb_create_bool(val))
create_value(val::T) where {T <: Int8} = Value(duckdb_create_int8(val))
create_value(val::T) where {T <: Int16} = Value(duckdb_create_int16(val))
create_value(val::T) where {T <: Int32} = Value(duckdb_create_int32(val))
create_value(val::T) where {T <: Int64} = Value(duckdb_create_int64(val))
create_value(val::T) where {T <: Int128} = Value(duckdb_create_hugeint(val))
create_value(val::T) where {T <: UInt8} = Value(duckdb_create_uint8(val))
create_value(val::T) where {T <: UInt16} = Value(duckdb_create_uint16(val))
create_value(val::T) where {T <: UInt32} = Value(duckdb_create_uint32(val))
create_value(val::T) where {T <: UInt64} = Value(duckdb_create_uint64(val))
create_value(val::T) where {T <: UInt128} = Value(duckdb_create_uhugeint(val))
create_value(val::T) where {T <: Float32} = Value(duckdb_create_float(val))
create_value(val::T) where {T <: Float64} = Value(duckdb_create_double(val))
create_value(val::T) where {T <: Date} =
    Value(duckdb_create_date(Dates.date2epochdays(val) - ROUNDING_EPOCH_TO_UNIX_EPOCH_DAYS))
create_value(val::T) where {T <: Time} = Value(duckdb_create_time(Dates.value(val) ÷ 1000))
create_value(val::T) where {T <: DateTime} =
    Value(duckdb_create_timestamp((Dates.datetime2epochms(val) - ROUNDING_EPOCH_TO_UNIX_EPOCH_MS) * 1000))
create_value(val::T) where {T <: AbstractString} = Value(duckdb_create_varchar_length(val, length(val)))
function create_value(val::AbstractVector{T}) where {T}
    type = create_logical_type(T)
    values = create_value.(val)
    return Value(duckdb_create_list_value(type.handle, map(x -> x.handle, values), length(values)))
end
function create_value(val::T) where {T}
    throw(NotImplementedException("Unsupported type for getvalue"))
end

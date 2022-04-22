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


create_value(val::T) where {T <: Int64} = Value(duckdb_create_int64(val))
create_value(val::T) where {T <: AbstractString} = Value(duckdb_create_varchar_length(val, length(val)))
function create_value(val::T) where {T}
    throw(NotImplementedException("Unsupported type for getvalue"))
end

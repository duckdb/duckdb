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

GetValue(val::Value, ::Type{T}) where {T <: Int64} = duckdb_get_int64(val.handle)
function GetValue(val::Value, ::Type{T}) where {T}
    throw(NotImplementedException("Unsupported type for GetValue"))
end

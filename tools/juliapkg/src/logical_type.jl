"""
DuckDB type
"""
mutable struct LogicalType
    handle::duckdb_logical_type

    function LogicalType(type::DUCKDB_TYPE)
        handle = duckdb_create_logical_type(type)
        result = new(handle)
        finalizer(_destroy_type, result)
        return result
    end
    function LogicalType(handle::duckdb_logical_type)
        result = new(handle)
        finalizer(_destroy_type, result)
        return result
    end
end

CreateLogicalType(::Type{T}) where {T <: String} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_VARCHAR)
CreateLogicalType(::Type{T}) where {T <: Int64} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_BIGINT)

function CreateLogicalType(::Type{T}) where {T}
    throw(NotImplementedException("Unsupported type for CreateLogicalType"))
end

function GetTypeId(type::LogicalType)
	return duckdb_get_type_id(type.handle)
end

function GetInternalTypeId(type::LogicalType)
	type_id = GetTypeId(type)
	if type_id == DUCKDB_TYPE_DECIMAL
		type_id = duckdb_decimal_internal_type(type.handle)
	end
	return type_id
end

function GetDecimalScale(type::LogicalType)
	return duckdb_decimal_scale(type.handle)
end

function _destroy_type(type::LogicalType)
    if type.handle != C_NULL
        duckdb_destroy_logical_type(type.handle)
    end
    type.handle = C_NULL
    return
end

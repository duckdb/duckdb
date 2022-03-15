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

function _destroy_type(type::LogicalType)
    if type.handle != C_NULL
        duckdb_destroy_logical_type(type.handle)
    end
    type.handle = C_NULL
    return
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
    elseif type_id == DUCKDB_TYPE_ENUM
        type_id = duckdb_enum_internal_type(type.handle)
    end
    return type_id
end

function GetDecimalScale(type::LogicalType)
    return duckdb_decimal_scale(type.handle)
end

function GetEnumDictionary(type::LogicalType)
    dict::Vector{String} = Vector{String}()
    dict_size = duckdb_enum_dictionary_size(type.handle)
    for i in 1:dict_size
        val = duckdb_enum_dictionary_value(type.handle, i)
        str_val = String(unsafe_string(val))
        push!(dict, str_val)
        duckdb_free(val)
    end
    return dict
end

function GetListChildType(type::LogicalType)
    return LogicalType(duckdb_list_type_child_type(type.handle))
end

function GetStructChildCount(type::LogicalType)
    return duckdb_struct_type_child_count(type.handle)
end

function GetStructChildName(type::LogicalType, index::UInt64)
    val = duckdb_struct_type_child_name(type.handle, index)
    result = unsafe_string(val)
    duckdb_free(val)
    return result
end

function GetStructChildType(type::LogicalType, index::UInt64)
    return LogicalType(duckdb_struct_type_child_type(type.handle, index))
end

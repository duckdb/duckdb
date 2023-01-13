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

create_logical_type(::Type{T}) where {T <: String} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_VARCHAR)
create_logical_type(::Type{T}) where {T <: Bool} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_BOOLEAN)
create_logical_type(::Type{T}) where {T <: Int8} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_TINYINT)
create_logical_type(::Type{T}) where {T <: Int16} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_SMALLINT)
create_logical_type(::Type{T}) where {T <: Int32} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_INTEGER)
create_logical_type(::Type{T}) where {T <: Int64} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_BIGINT)
create_logical_type(::Type{T}) where {T <: UInt8} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_UTINYINT)
create_logical_type(::Type{T}) where {T <: UInt16} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_USMALLINT)
create_logical_type(::Type{T}) where {T <: UInt32} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_UINTEGER)
create_logical_type(::Type{T}) where {T <: UInt64} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_UBIGINT)
create_logical_type(::Type{T}) where {T <: Float32} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_FLOAT)
create_logical_type(::Type{T}) where {T <: Float64} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_DOUBLE)
create_logical_type(::Type{T}) where {T <: Date} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_DATE)
create_logical_type(::Type{T}) where {T <: Time} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_TIME)
create_logical_type(::Type{T}) where {T <: DateTime} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_TIMESTAMP)
create_logical_type(::Type{T}) where {T <: AbstractString} = DuckDB.LogicalType(DuckDB.DUCKDB_TYPE_VARCHAR)
function create_logical_type(::Type{T}) where {T <: FixedDecimal}
    int_type = T.parameters[1]
    width = 0
    scale = T.parameters[2]
    if int_type == Int16
        width = 4
    elseif int_type == Int32
        width = 9
    elseif int_type == Int64
        width = 18
    elseif int_type == Int128
        width = 38
    else
        throw(NotImplementedException("Unsupported internal type for decimal"))
    end
    return DuckDB.LogicalType(duckdb_create_decimal_type(width, scale))
end

function create_logical_type(::Type{T}) where {T}
    throw(NotImplementedException("Unsupported type for create_logical_type"))
end

function get_type_id(type::LogicalType)
    return duckdb_get_type_id(type.handle)
end

function get_internal_type_id(type::LogicalType)
    type_id = get_type_id(type)
    if type_id == DUCKDB_TYPE_DECIMAL
        type_id = duckdb_decimal_internal_type(type.handle)
    elseif type_id == DUCKDB_TYPE_ENUM
        type_id = duckdb_enum_internal_type(type.handle)
    end
    return type_id
end

function get_decimal_scale(type::LogicalType)
    return duckdb_decimal_scale(type.handle)
end

function get_enum_dictionary(type::LogicalType)
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

function get_list_child_type(type::LogicalType)
    return LogicalType(duckdb_list_type_child_type(type.handle))
end

##===--------------------------------------------------------------------===##
## Struct methods
##===--------------------------------------------------------------------===##

function get_struct_child_count(type::LogicalType)
    return duckdb_struct_type_child_count(type.handle)
end


function get_struct_child_name(type::LogicalType, index::UInt64)
    val = duckdb_struct_type_child_name(type.handle, index)
    result = unsafe_string(val)
    duckdb_free(val)
    return result
end

function get_struct_child_type(type::LogicalType, index::UInt64)
    return LogicalType(duckdb_struct_type_child_type(type.handle, index))
end

##===--------------------------------------------------------------------===##
## Union methods
##===--------------------------------------------------------------------===##

function get_union_member_count(type::LogicalType)
    return duckdb_union_type_member_count(type.handle)
end

function get_union_member_name(type::LogicalType, index::UInt64)
    val = duckdb_union_type_member_name(type.handle, index)
    result = unsafe_string(val)
    duckdb_free(val)
    return result
end

function get_union_member_type(type::LogicalType, index::UInt64)
    return LogicalType(duckdb_union_type_member_type(type.handle, index))
end


const duckdb_database = Ptr{Cvoid}
const duckdb_config = Ptr{Cvoid}
const duckdb_connection = Ptr{Cvoid}
const duckdb_prepared_statement = Ptr{Cvoid}
const duckdb_pending_result = Ptr{Cvoid}
const duckdb_logical_type = Ptr{Cvoid}
const duckdb_data_chunk = Ptr{Cvoid}
const duckdb_vector = Ptr{Cvoid}
const duckdb_appender = Ptr{Cvoid}
const duckdb_logical_type = Ptr{Cvoid}
const duckdb_value = Ptr{Cvoid}
const duckdb_table_function = Ptr{Cvoid}
const duckdb_bind_info = Ptr{Cvoid}
const duckdb_init_info = Ptr{Cvoid}
const duckdb_function_info = Ptr{Cvoid}
const duckdb_replacement_scan_info = Ptr{Cvoid}
const duckdb_task_state = Ptr{Cvoid}

const duckdb_state = Int32;
const DuckDBSuccess = 0;
const DuckDBError = 1;

const duckdb_pending_state = Int32;
const DUCKDB_PENDING_RESULT_READY = 0;
const DUCKDB_PENDING_RESULT_NOT_READY = 1;
const DUCKDB_PENDING_ERROR = 2;
const DUCKDB_PENDING_NO_TASKS_AVAILABLE = 3;

@enum DUCKDB_TYPE_::UInt32 begin
    DUCKDB_TYPE_INVALID = 0
    DUCKDB_TYPE_BOOLEAN
    DUCKDB_TYPE_TINYINT
    DUCKDB_TYPE_SMALLINT
    DUCKDB_TYPE_INTEGER
    DUCKDB_TYPE_BIGINT
    DUCKDB_TYPE_UTINYINT
    DUCKDB_TYPE_USMALLINT
    DUCKDB_TYPE_UINTEGER
    DUCKDB_TYPE_UBIGINT
    DUCKDB_TYPE_FLOAT
    DUCKDB_TYPE_DOUBLE
    DUCKDB_TYPE_TIMESTAMP
    DUCKDB_TYPE_DATE
    DUCKDB_TYPE_TIME
    DUCKDB_TYPE_INTERVAL
    DUCKDB_TYPE_HUGEINT
    DUCKDB_TYPE_VARCHAR
    DUCKDB_TYPE_BLOB
    DUCKDB_TYPE_DECIMAL
    DUCKDB_TYPE_TIMESTAMP_S
    DUCKDB_TYPE_TIMESTAMP_MS
    DUCKDB_TYPE_TIMESTAMP_NS
    DUCKDB_TYPE_ENUM
    DUCKDB_TYPE_LIST
    DUCKDB_TYPE_STRUCT
    DUCKDB_TYPE_MAP
    DUCKDB_TYPE_UUID
    DUCKDB_TYPE_UNION
    DUCKDB_TYPE_BIT
end

const DUCKDB_TYPE = DUCKDB_TYPE_

"""
Days are stored as days since 1970-01-01\n
Use the duckdb_from_date/duckdb_to_date function to extract individual information

"""
struct duckdb_date
    days::Int32
end

struct duckdb_date_struct
    year::Int32
    month::Int8
    day::Int8
end

"""
Time is stored as microseconds since 00:00:00\n
Use the duckdb_from_time/duckdb_to_time function to extract individual information

"""
struct duckdb_time
    micros::Int64
end

struct duckdb_time_struct
    hour::Int8
    min::Int8
    sec::Int8
    micros::Int32
end

"""
Timestamps are stored as microseconds since 1970-01-01\n
Use the duckdb_from_timestamp/duckdb_to_timestamp function to extract individual information

"""
struct duckdb_timestamp
    micros::Int64
end

struct duckdb_timestamp_struct
    date::Ref{duckdb_date_struct}
    time::Ref{duckdb_time_struct}
end

struct duckdb_interval
    months::Int32
    days::Int32
    micros::Int64
end

"""
Hugeints are composed in a (lower, upper) component\n
The value of the hugeint is upper * 2^64 + lower\n
For easy usage, the functions duckdb_hugeint_to_double/duckdb_double_to_hugeint are recommended

"""
struct duckdb_hugeint
    lower::UInt64
    upper::Int64
end

struct duckdb_string_t
    length::UInt32
    data::NTuple{12, UInt8}
end

struct duckdb_list_entry_t
    offset::UInt64
    length::UInt64
end

STRING_INLINE_LENGTH = 12

struct duckdb_column
    __deprecated_data::Ptr{Cvoid}
    __deprecated_nullmask::Ptr{UInt8}
    __deprecated_type::Ptr{DUCKDB_TYPE}
    __deprecated_name::Ptr{UInt8}
    internal_data::Ptr{Cvoid}
end

struct duckdb_result
    __deprecated_column_count::Ptr{UInt64}
    __deprecated_row_count::Ptr{UInt64}
    __deprecated_rows_changed::Ptr{UInt64}
    __deprecated_columns::Ptr{duckdb_column}
    __deprecated_error_message::Ptr{UInt8}
    internal_data::Ptr{Cvoid}
end

INTERNAL_TYPE_MAP = Dict(
    DUCKDB_TYPE_BOOLEAN => Bool,
    DUCKDB_TYPE_TINYINT => Int8,
    DUCKDB_TYPE_SMALLINT => Int16,
    DUCKDB_TYPE_INTEGER => Int32,
    DUCKDB_TYPE_BIGINT => Int64,
    DUCKDB_TYPE_UTINYINT => UInt8,
    DUCKDB_TYPE_USMALLINT => UInt16,
    DUCKDB_TYPE_UINTEGER => UInt32,
    DUCKDB_TYPE_UBIGINT => UInt64,
    DUCKDB_TYPE_FLOAT => Float32,
    DUCKDB_TYPE_DOUBLE => Float64,
    DUCKDB_TYPE_TIMESTAMP => Int64,
    DUCKDB_TYPE_TIMESTAMP_S => Int64,
    DUCKDB_TYPE_TIMESTAMP_MS => Int64,
    DUCKDB_TYPE_TIMESTAMP_NS => Int64,
    DUCKDB_TYPE_DATE => Int32,
    DUCKDB_TYPE_TIME => Int64,
    DUCKDB_TYPE_INTERVAL => duckdb_interval,
    DUCKDB_TYPE_HUGEINT => duckdb_hugeint,
    DUCKDB_TYPE_UUID => duckdb_hugeint,
    DUCKDB_TYPE_VARCHAR => duckdb_string_t,
    DUCKDB_TYPE_BLOB => duckdb_string_t,
    DUCKDB_TYPE_BIT => duckdb_string_t,
    DUCKDB_TYPE_UUID => duckdb_hugeint,
    DUCKDB_TYPE_LIST => duckdb_list_entry_t,
    DUCKDB_TYPE_STRUCT => Cvoid,
    DUCKDB_TYPE_MAP => duckdb_list_entry_t,
    DUCKDB_TYPE_UNION => Cvoid
)

JULIA_TYPE_MAP = Dict(
    DUCKDB_TYPE_INVALID => Missing,
    DUCKDB_TYPE_BOOLEAN => Bool,
    DUCKDB_TYPE_TINYINT => Int8,
    DUCKDB_TYPE_SMALLINT => Int16,
    DUCKDB_TYPE_INTEGER => Int32,
    DUCKDB_TYPE_BIGINT => Int64,
    DUCKDB_TYPE_HUGEINT => Int128,
    DUCKDB_TYPE_UTINYINT => UInt8,
    DUCKDB_TYPE_USMALLINT => UInt16,
    DUCKDB_TYPE_UINTEGER => UInt32,
    DUCKDB_TYPE_UBIGINT => UInt64,
    DUCKDB_TYPE_FLOAT => Float32,
    DUCKDB_TYPE_DOUBLE => Float64,
    DUCKDB_TYPE_DATE => Date,
    DUCKDB_TYPE_TIME => Time,
    DUCKDB_TYPE_TIMESTAMP => DateTime,
    DUCKDB_TYPE_TIMESTAMP_S => DateTime,
    DUCKDB_TYPE_TIMESTAMP_MS => DateTime,
    DUCKDB_TYPE_TIMESTAMP_NS => DateTime,
    DUCKDB_TYPE_INTERVAL => Dates.CompoundPeriod,
    DUCKDB_TYPE_UUID => UUID,
    DUCKDB_TYPE_VARCHAR => String,
    DUCKDB_TYPE_ENUM => String,
    DUCKDB_TYPE_BLOB => Base.CodeUnits{UInt8, String},
    DUCKDB_TYPE_BIT => Base.CodeUnits{UInt8, String},
    DUCKDB_TYPE_MAP => Dict
)

# convert a DuckDB type into Julia equivalent
function duckdb_type_to_internal_type(x::DUCKDB_TYPE)
    if !haskey(INTERNAL_TYPE_MAP, x)
        throw(NotImplementedException(string("Unsupported type for duckdb_type_to_internal_type: ", x)))
    end
    return INTERNAL_TYPE_MAP[x]
end

function duckdb_type_to_julia_type(x)
    type_id = get_type_id(x)
    if type_id == DUCKDB_TYPE_DECIMAL
        internal_type_id = get_internal_type_id(x)
        scale = get_decimal_scale(x)
        if internal_type_id == DUCKDB_TYPE_SMALLINT
            return FixedDecimal{Int16, scale}
        elseif internal_type_id == DUCKDB_TYPE_INTEGER
            return FixedDecimal{Int32, scale}
        elseif internal_type_id == DUCKDB_TYPE_BIGINT
            return FixedDecimal{Int64, scale}
        elseif internal_type_id == DUCKDB_TYPE_HUGEINT
            return FixedDecimal{Int128, scale}
        else
            throw(NotImplementedException("Unimplemented internal type for decimal"))
        end
    elseif type_id == DUCKDB_TYPE_LIST
        return Vector{Union{Missing, duckdb_type_to_julia_type(get_list_child_type(x))}}
    elseif type_id == DUCKDB_TYPE_STRUCT
        child_count = get_struct_child_count(x)
        struct_names::Vector{Symbol} = Vector()
        for i in 1:child_count
            child_name::Symbol = Symbol(get_struct_child_name(x, i))
            push!(struct_names, child_name)
        end
        struct_names_tuple = Tuple(x for x in struct_names)
        return Union{Missing, NamedTuple{struct_names_tuple}}
    elseif type_id == DUCKDB_TYPE_UNION
        member_count = get_union_member_count(x)
        member_types::Vector{DataType} = Vector()
        for i in 1:member_count
            member_type::DataType = duckdb_type_to_julia_type(get_union_member_type(x, i))
            push!(member_types, member_type)
        end
        return Union{Missing, member_types...}
    end
    if !haskey(JULIA_TYPE_MAP, type_id)
        throw(NotImplementedException(string("Unsupported type for duckdb_type_to_julia_type: ", type_id)))
    end
    return JULIA_TYPE_MAP[type_id]
end

const ROUNDING_EPOCH_TO_UNIX_EPOCH_DAYS = 719528
const ROUNDING_EPOCH_TO_UNIX_EPOCH_MS = 62167219200000

sym(ptr) = ccall(:jl_symbol, Ref{Symbol}, (Ptr{UInt8},), ptr)

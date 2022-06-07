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

struct duckdb_blob
    data::Cvoid
    size::UInt64
end

struct duckdb_column
    data::Ptr{Cvoid}
    nullmask::Ptr{UInt8}
    type::Ptr{DUCKDB_TYPE}
    name::Ptr{UInt8}
    internal_data::Ptr{Cvoid}
end

struct duckdb_result
    column_count::Ptr{UInt64}
    row_count::Ptr{UInt64}
    rows_changed::Ptr{UInt64}
    columns::Ptr{duckdb_column}
    error_message::Ptr{UInt8}
    internal_data::Ptr{Cvoid}
end

DUCKDB_TYPES = Dict(
    DUCKDB_TYPE_BOOLEAN => UInt8,
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
    DUCKDB_TYPE_DATE => Int32,
    DUCKDB_TYPE_TIME => Int64,
    DUCKDB_TYPE_INTERVAL => duckdb_interval,
    DUCKDB_TYPE_HUGEINT => duckdb_hugeint,
    DUCKDB_TYPE_VARCHAR => Ptr{UInt8},
    DUCKDB_TYPE_BLOB => duckdb_blob,
)

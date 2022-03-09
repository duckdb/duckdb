"""
DuckDB data chunk
"""
mutable struct DataChunk
    handle::duckdb_data_chunk

    function DataChunk(handle::duckdb_data_chunk, destroy::Bool)
        result = new(handle)
        if destroy
            finalizer(_destroy_data_chunk, result)
        end
        return result
    end
end

function GetColumnCount(chunk::DataChunk)
    return duckdb_data_chunk_get_column_count(chunk.handle)
end

function GetSize(chunk::DataChunk)
    return duckdb_data_chunk_get_size(chunk.handle)
end

function SetSize(chunk::DataChunk, size::Int64)
    return duckdb_data_chunk_set_size(chunk.handle, size)
end

function GetArray(chunk::DataChunk, col_idx::Int64, ::Type{T})::Vector{T} where {T}
    if col_idx < 1 || col_idx > GetColumnCount(chunk)
        throw(
            InvalidInputException(
                string(
                    "GetArray column index ",
                    col_idx,
                    " out of range, expected value between 1 and ",
                    GetColumnCount(chunk)
                )
            )
        )
    end
    raw_ptr = duckdb_data_chunk_get_data(chunk.handle, col_idx - 1)
    ptr = Base.unsafe_convert(Ptr{T}, raw_ptr)
    return unsafe_wrap(Vector{T}, ptr, VECTOR_SIZE, own = false)
end

function GetValidity(chunk::DataChunk, col_idx::Int64)::ValidityMask
    duckdb_data_chunk_ensure_validity_writable(chunk.handle, col_idx)
    validity_ptr = duckdb_data_chunk_get_validity(chunk.handle, col_idx)
    ptr = Base.unsafe_convert(Ptr{UInt64}, validity_ptr)
    validity_vector = unsafe_wrap(Vector{UInt64}, ptr, VECTOR_SIZE ÷ BITS_PER_VALUE, own = false)
    return ValidityMask(validity_vector)
end

function GetArrays(types::Vector{Type}, chunk::DataChunk)::Vector{Ptr{Cvoid}}
    column_count = GetColumnCount(chunk)
    result::Vector{Ptr{Cvoid}} = []
    for i::Int64 in 1:column_count
        push!(result, duckdb_data_chunk_get_data(chunk.handle, i - 1))
    end
    return result
end

function GetValidityMasks(chunk::DataChunk)::Vector{ValidityMask}
    column_count = GetColumnCount(chunk)
    result::Vector{ValidityMask} = []
    for i::Int64 in 1:column_count
        push!(result, GetValidity(chunk, i - 1))
    end
    return result
end

# this is only required when we own the data chunk
function _destroy_data_chunk(chunk::DataChunk)
    if chunk.handle != C_NULL
        duckdb_destroy_data_chunk(chunk.handle)
    end
    return chunk.handle = C_NULL
end

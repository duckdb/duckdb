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

function GetVector(chunk::DataChunk, col_idx::Int64)::Vec
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
    return Vec(duckdb_data_chunk_get_vector(chunk.handle, col_idx))
end

function GetArray(chunk::DataChunk, col_idx::Int64, ::Type{T})::Vector{T} where {T}
    return GetArray(GetVector(chunk, col_idx), T)
end

function GetValidity(chunk::DataChunk, col_idx::Int64)::ValidityMask
    return GetValidity(GetVector(chunk, col_idx))
end

function AllValid(chunk::DataChunk, col_idx::Int64)
    return AllValid(GetVector(chunk, col_idx))
end

# this is only required when we own the data chunk
function _destroy_data_chunk(chunk::DataChunk)
    if chunk.handle != C_NULL
        duckdb_destroy_data_chunk(chunk.handle)
    end
    return chunk.handle = C_NULL
end

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

function get_column_count(chunk::DataChunk)
    return duckdb_data_chunk_get_column_count(chunk.handle)
end

function get_size(chunk::DataChunk)
    return duckdb_data_chunk_get_size(chunk.handle)
end

function set_size(chunk::DataChunk, size::Int64)
    return duckdb_data_chunk_set_size(chunk.handle, size)
end

function get_vector(chunk::DataChunk, col_idx::Int64)::Vec
    if col_idx < 1 || col_idx > get_column_count(chunk)
        throw(
            InvalidInputException(
                string(
                    "get_array column index ",
                    col_idx,
                    " out of range, expected value between 1 and ",
                    get_column_count(chunk)
                )
            )
        )
    end
    return Vec(duckdb_data_chunk_get_vector(chunk.handle, col_idx))
end

function get_array(chunk::DataChunk, col_idx::Int64, ::Type{T})::Vector{T} where {T}
    return get_array(get_vector(chunk, col_idx), T)
end

function get_validity(chunk::DataChunk, col_idx::Int64)::ValidityMask
    return get_validity(get_vector(chunk, col_idx))
end

function all_valid(chunk::DataChunk, col_idx::Int64)
    return all_valid(get_vector(chunk, col_idx), get_size(chunk))
end

# this is only required when we own the data chunk
function _destroy_data_chunk(chunk::DataChunk)
    if chunk.handle != C_NULL
        duckdb_destroy_data_chunk(chunk.handle)
    end
    return chunk.handle = C_NULL
end

function destroy_data_chunk(chunk::DataChunk)
    return _destroy_data_chunk(chunk)
end

"""
DuckDB value
"""
mutable struct DataChunk
    handle::duckdb_data_chunk

    function DataChunk(handle::duckdb_data_chunk)
        result = new(handle)
        return result
    end
end

function GetSize(chunk::DataChunk)
	return duckdb_data_chunk_get_size(chunk.handle)
end

function SetSize(chunk::DataChunk, size::Int64)
	duckdb_data_chunk_set_size(chunk.handle, size)
end

function GetArray(chunk::DataChunk, col_idx::Int64, ::Type{T}) where {T}
	raw_ptr = duckdb_data_chunk_get_data(chunk.handle, col_idx)
	ptr = Base.unsafe_convert(Ptr{T}, raw_ptr)
	return unsafe_wrap(Vector{T}, ptr, VECTOR_SIZE, own=false)
end

#
# function _destroy_data_chunk(chunk::DataChunk)
#     if chunk.handle != C_NULL
#         duckdb_destroy_data_chunk(chunk.handle)
#     end
#     chunk.handle = C_NULL
# end

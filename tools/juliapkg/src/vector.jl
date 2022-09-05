"""
DuckDB vector
"""
struct Vec
    handle::duckdb_vector

    function Vec(handle::duckdb_vector)
        result = new(handle)
        return result
    end
end

function get_array(vector::Vec, ::Type{T})::Vector{T} where {T}
    raw_ptr = duckdb_vector_get_data(vector.handle)
    ptr = Base.unsafe_convert(Ptr{T}, raw_ptr)
    return unsafe_wrap(Vector{T}, ptr, VECTOR_SIZE, own = false)
end

function get_validity(vector::Vec)::ValidityMask
    duckdb_vector_ensure_validity_writable(vector.handle)
    validity_ptr = duckdb_vector_get_validity(vector.handle)
    ptr = Base.unsafe_convert(Ptr{UInt64}, validity_ptr)
    validity_vector = unsafe_wrap(Vector{UInt64}, ptr, VECTOR_SIZE ÷ BITS_PER_VALUE, own = false)
    return ValidityMask(validity_vector)
end

function all_valid(vector::Vec)::Bool
    return duckdb_vector_get_validity(vector.handle) == C_NULL
end

function list_child(vector::Vec)::Vec
    return Vec(duckdb_list_vector_get_child(vector.handle))
end

function list_size(vector::Vec)::UInt64
    return duckdb_list_vector_get_size(vector.handle)
end

function struct_child(vector::Vec, index::UInt64)::Vec
    return Vec(duckdb_struct_vector_get_child(vector.handle, index))
end

function assign_string_element(vector::Vec, index::Int64, str::String)
    return duckdb_vector_assign_string_element_len(vector.handle, index, str, sizeof(str))
end

function assign_string_element(vector::Vec, index::Int64, str::AbstractString)
    return duckdb_vector_assign_string_element_len(vector.handle, index, str, sizeof(str))
end

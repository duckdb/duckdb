"""
DuckDB vector
"""
mutable struct Vec
    handle::duckdb_vector

    function Vec(handle::duckdb_vector)
        result = new(handle)
        return result
    end
end

function GetArray(vector::Vec, ::Type{T})::Vector{T} where {T}
    raw_ptr = duckdb_vector_get_data(vector.handle)
    ptr = Base.unsafe_convert(Ptr{T}, raw_ptr)
    return unsafe_wrap(Vector{T}, ptr, VECTOR_SIZE, own = false)
end

function GetValidity(vector::Vec)::ValidityMask
    duckdb_vector_ensure_validity_writable(vector.handle)
    validity_ptr = duckdb_vector_get_validity(vector.handle)
    ptr = Base.unsafe_convert(Ptr{UInt64}, validity_ptr)
    validity_vector = unsafe_wrap(Vector{UInt64}, ptr, VECTOR_SIZE ÷ BITS_PER_VALUE, own = false)
    return ValidityMask(validity_vector)
end

function AllValid(vector::Vec)::Bool
	return duckdb_vector_get_validity(vector.handle) == C_NULL
end

function ListChild(vector::Vec)::Vec
	return Vec(duckdb_list_vector_get_child(vector.handle))
end

function ListSize(vector::Vec)::UInt64
	return duckdb_list_vector_get_size(vector.handle)
end
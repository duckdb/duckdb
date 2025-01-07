

"""
    duckdb_free(s::Cstring)

Free a Cstring allocated by DuckDB. This function is a wrapper around `duckdb_free`.
"""
function duckdb_free(s::Cstring)
    p = pointer(s)
    return duckdb_free(p)
end

"""
Retrieves the member vector of a union vector.

The resulting vector is valid as long as the parent vector is valid.

* vector: The vector
* index: The member index
* returns: The member vector
"""
function duckdb_union_vector_get_member(vector, index)
    return ccall(
        (:duckdb_struct_vector_get_child, libduckdb),
        duckdb_vector,
        (duckdb_vector, UInt64),
        vector,
        1 + (index - 1)
    )
end

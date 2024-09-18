# ctypes.jl
const duckdb_scalar_function = Ptr{Cvoid}

# api.jl scalar functions
function duckdb_create_scalar_function()
    return ccall((:duckdb_create_scalar_function, libduckdb), duckdb_scalar_function, ())
end

function duckdb_destroy_scalar_function(func)
    return ccall((:duckdb_destroy_scalar_function, libduckdb), Cvoid, (Ref{duckdb_scalar_function},), func)
end

function duckdb_scalar_function_set_name(func, name)
    return ccall((:duckdb_scalar_function_set_name, libduckdb), Cvoid, (duckdb_scalar_function, Ptr{UInt8}), func, name)
end

function duckdb_scalar_function_add_parameter(func, type)
    return ccall(
        (:duckdb_scalar_function_add_parameter, libduckdb),
        Cvoid,
        (duckdb_scalar_function, duckdb_logical_type),
        func,
        type
    )
end

function duckdb_scalar_function_set_return_type(func, type)
    return ccall(
        (:duckdb_scalar_function_set_return_type, libduckdb),
        Cvoid,
        (duckdb_scalar_function, duckdb_logical_type),
        func,
        type
    )
end

function duckdb_scalar_function_set_function(scalar_func, func)
    return ccall(
        (:duckdb_scalar_function_set_function, libduckdb),
        Cvoid,
        (duckdb_scalar_function, Ptr{Cvoid}),
        scalar_func,
        func
    )
end

function duckdb_register_scalar_function(con, func)
    return ccall(
        (:duckdb_register_scalar_function, libduckdb),
        Int32,
        (duckdb_connection, duckdb_scalar_function),
        con,
        func
    )
end

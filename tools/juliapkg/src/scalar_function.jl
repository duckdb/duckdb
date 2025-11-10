#=
//===--------------------------------------------------------------------===//
// Scalar Function
//===--------------------------------------------------------------------===//
=#
"""
    ScalarFunction(
        name::AbstractString,
        parameters::Vector{DataType},
        return_type::DataType,
        func,
        wrapper = nothing
    )

Creates a new scalar function object. It is recommended to use the `@create_scalar_function` 
macro to create a new scalar function instead of calling this constructor directly.

# Arguments
- `name::AbstractString`: The name of the function.
- `parameters::Vector{DataType}`: The data types of the parameters.
- `return_type::DataType`: The return type of the function.
- `func`: The function to be called.
- `wrapper`: The wrapper function that is used to call the function from DuckDB.
- `wrapper_id`: A unique id for the wrapper function.

See also [`register_scalar_function`](@ref), [`@create_scalar_function`](@ref)
"""
mutable struct ScalarFunction
    handle::duckdb_scalar_function
    name::AbstractString
    parameters::Vector{DataType}
    return_type::DataType
    logical_parameters::Vector{LogicalType}
    logical_return_type::LogicalType
    func::Function
    wrapper::Union{Nothing, Function} # the wrapper function to hold a reference to it to prevent GC
    wrapper_id::Union{Nothing, UInt64}

    function ScalarFunction(
        name::AbstractString,
        parameters::Vector{DataType},
        return_type::DataType,
        func,
        wrapper = nothing,
        wrapper_id = nothing
    )
        handle = duckdb_create_scalar_function()
        duckdb_scalar_function_set_name(handle, name)

        logical_parameters = Vector{LogicalType}()
        for parameter_type in parameters
            push!(logical_parameters, create_logical_type(parameter_type))
        end
        logical_return_type = create_logical_type(return_type)

        for param in logical_parameters
            duckdb_scalar_function_add_parameter(handle, param.handle)
        end
        duckdb_scalar_function_set_return_type(handle, logical_return_type.handle)
        result = new(
            handle,
            name,
            parameters,
            return_type,
            logical_parameters,
            logical_return_type,
            func,
            wrapper,
            wrapper_id
        )
        finalizer(_destroy_scalar_function, result)
        duckdb_scalar_function_set_extra_info(handle, pointer_from_objref(result), C_NULL)

        return result
    end

end

name(func::ScalarFunction) = func.name
signature(func::ScalarFunction) = string(func.name, "(", join(func.parameters, ", "), ") -> ", func.return_type)

function Base.show(io::IO, func::ScalarFunction)
    print(io, "DuckDB.ScalarFunction(", signature(func), ")")
    return
end

function _destroy_scalar_function(func::ScalarFunction)
    # disconnect from DB
    if func.handle != C_NULL
        duckdb_destroy_scalar_function(func.handle)
    end

    # remove the wrapper from the cache
    if func.wrapper_id !== nothing && func.wrapper_id in keys(_UDF_WRAPPER_CACHE)
        delete!(_UDF_WRAPPER_CACHE, func.wrapper_id)
    end

    func.handle = C_NULL
    return
end


"""
    register_scalar_function(db::DB, fun::ScalarFunction)
    register_scalar_function(con::Connection, fun::ScalarFunction)

Register a scalar function in the database.
"""
register_scalar_function(db::DB, fun::ScalarFunction) = register_scalar_function(db.main_connection, fun)
function register_scalar_function(con::Connection, fun::ScalarFunction)

    if fun.name in keys(con.db.scalar_functions)
        throw(ArgumentError(string("Scalar function \"", fun.name, "\" already registered")))
    end

    result = duckdb_register_scalar_function(con.handle, fun.handle)
    if result != DuckDBSuccess
        throw(ArgumentError(string("Failed to register scalar function \"", fun.name, "\"")))
    end

    con.db.scalar_functions[fun.name] = fun
    return
end


# %% --- Scalar Function Macro ------------------------------------------ #

"""
    name, at, rt = _udf_parse_function_expr(expr::Expr)

Parses a function expression and returns the function name, parameters and return type.
The parameters are turned as a vector of argument name, argument type tuples.

# Example

```julia
expr = :(my_sum(a::Int, b::Int)::Int)
name, at, rt = _udf_parse_function_expr(expr)
```
"""
function _udf_parse_function_expr(expr::Expr)

    function parse_parameter(parameter_expr::Expr)
        parameter_expr.head === :(::) || throw(ArgumentError("parameter_expr must be a type annotation"))
        parameter, parameter_type = parameter_expr.args

        if !isa(parameter, Symbol)
            throw(ArgumentError("parameter name must be a symbol"))
        end

        # if !isa(parameter_type, Symbol)
        #     throw(ArgumentError("parameter_type must be a symbol"))
        # end

        return parameter, parameter_type
    end

    expr.head === :(::) ||
        throw(ArgumentError("expr must be a typed function signature, e.g. func(a::Int, b::String)::Int"))
    inner, return_type = expr.args

    # parse inner
    if !isa(inner, Expr)
        throw(ArgumentError("inner must be an expression"))
    end

    inner.head === :call ||
        throw(ArgumentError("expr must be a typed function signature, e.g. func(a::Int, b::String)::Int"))
    func_name = inner.args[1]
    parameters = parse_parameter.(inner.args[2:(end)])
    return func_name, parameters, return_type
end

function _udf_generate_conversion_expressions(parameters, logical_type, convert, var_name, chunk_name)

    # Example:
    # data_1 = convert(Int, LT[1], chunk, 1)
    var_names = [Symbol("$(var_name)_$(i)") for i in 1:length(parameters)]
    expressions = [
        Expr(:(=), var_names[i], Expr(:call, convert, p_type, Expr(:ref, logical_type, i), chunk_name, i)) for
        (i, (p_name, p_type)) in enumerate(parameters)
    ]
    return var_names, expressions
end




function _udf_generate_wrapper(func_expr, func_esc)
    index_name = :i
    log_param_types_name = :log_param_types
    log_return_type_name = :log_return_type

    # Parse the function definition, e.g. my_func(a::Int, b::String)::Int
    func, parameters, return_type = _udf_parse_function_expr(func_expr)
    func_name = string(func)


    # Generate expressions to unpack the data chunk:
    #   param_1 = convert(Int, LT[1], chunk, 1)
    #   param_2 = convert(Int, LT[2], chunk, 2)
    var_names, input_assignments =
        _udf_generate_conversion_expressions(parameters, log_param_types_name, :_udf_convert_chunk, :param, :chunk)


    # Generate the call expression: result = func(param_1, param_2, ...)
    call_args_loop = [:($var_name[$index_name]) for var_name in var_names]
    call_expr = Expr(:call, func_esc, call_args_loop...)

    # Generate the validity expression: get_validity(chunk, i)
    validity_expr_i = i -> Expr(:call, :get_validity, :chunk, i)
    validity_expr = Expr(:tuple, (validity_expr_i(i) for i in 1:length(parameters))...)

    return quote
        function (info::DuckDB.duckdb_function_info, input::DuckDB.duckdb_data_chunk, output::DuckDB.duckdb_vector)

            extra_info_ptr = DuckDB.duckdb_scalar_function_get_extra_info(info)
            scalar_func::DuckDB.ScalarFunction = unsafe_pointer_to_objref(extra_info_ptr)
            $log_param_types_name::Vector{LogicalType} = scalar_func.logical_parameters
            $log_return_type_name::LogicalType = scalar_func.logical_return_type

            try
                vec = Vec(output)
                chunk = DataChunk(input, false) # create a data chunk object, that does not own the data
                $(input_assignments...) # Assign the input values
                N = Int64(get_size(chunk))

                # initialize the result container, to avoid calling get_array() in the loop
                result_container = _udf_assign_result_init($return_type, vec)

                # Check data validity
                validity = $validity_expr
                chunk_is_valid = all(all_valid.(validity))
                result_validity = get_validity(vec)

                for $index_name in 1:N
                    if chunk_is_valid || all(isvalid(v, $index_name) for v in validity)
                        result::$return_type = $call_expr

                        # Hopefully this optimized away if the type has no missing values
                        if ismissing(result)
                            setinvalid(result_validity, $index_name)
                        else
                            _udf_assign_result!(result_container, $return_type, vec, result, $index_name)
                        end
                    else
                        setinvalid(result_validity, $index_name)
                    end
                end
                return nothing
            catch e
                duckdb_scalar_function_set_error(
                    info,
                    "Exception in " * signature(scalar_func) * ": " * get_exception_info()
                )
            end
        end
    end

end

"""
Internal storage to have globally accessible functions pointers.

HACK: This is a workaround to dynamically generate a function pointer on ALL architectures.
"""
const _UDF_WRAPPER_CACHE = Dict{UInt64, Function}()

function _udf_register_wrapper(id, wrapper)


    if id in keys(_UDF_WRAPPER_CACHE)
        throw(
            InvalidInputException(
                "A function with the same id has already been registered. This should not happen. Please report this issue."
            )
        )
    end

    _UDF_WRAPPER_CACHE[id] = wrapper

    # HACK: This is a workaround to dynamically generate a function pointer on ALL architectures
    # We need to delay the cfunction call until the moment wrapper function is generated
    fptr = QuoteNode(:(_UDF_WRAPPER_CACHE[$id]))
    cfunction_type = Ptr{Cvoid}
    rt = :Cvoid
    at = :(duckdb_function_info, duckdb_data_chunk, duckdb_vector)
    attr_svec = Expr(:call, GlobalRef(Core, :svec), at.args...)
    cfun = Expr(:cfunction, cfunction_type, fptr, rt, attr_svec, QuoteNode(:ccall))
    ptr = eval(cfun)
    return ptr
end




"""
    @create_scalar_function func_expr [func_ref]

Creates a new Scalar Function object that can be registered in a DuckDB database.

# Arguments
- `func_expr`: An expression that defines the function signature.
- `func_def`: An optional definition of the function or a closure. If omitted, it is assumed that a function with same name given in `func_expr` is defined in the global scope.
 

# Example

```julia
db = DuckDB.DB()
my_add(a,b) = a + b
fun = @create_scalar_function my_add(a::Int, b::Int)::Int 
DuckDB.register_scalar_function(db, fun) # Register UDF
```

"""
macro create_scalar_function(func_expr, func_ref = nothing)
    func, parameters, return_type = _udf_parse_function_expr(func_expr)
    if func_ref !== nothing
        func_esc = esc(func_ref)
    else
        func_esc = esc(func)
    end
    #@info "Create Scalar Function" func func_esc, parameters, return_type
    func_name = string(func)
    parameter_names = [p[1] for p in parameters]
    parameter_types = [p[2] for p in parameters]
    parameter_types_vec = Expr(:vect, parameter_types...) # create a vector expression, e.g. [Int, Int]
    wrapper_expr = _udf_generate_wrapper(func_expr, func_esc)

    id = hash((func_expr, rand(UInt64))) # generate a unique id for the function

    return quote
        local wrapper = $(wrapper_expr)
        local fun = ScalarFunction($func_name, $parameter_types_vec, $return_type, $func_esc, wrapper, $id)

        ptr = _udf_register_wrapper($id, wrapper)
        # Everything below only works in GLOBAL scope in the repl
        # ptr = @cfunction(fun.wrapper, Cvoid, (duckdb_function_info, duckdb_data_chunk, duckdb_vector))
        duckdb_scalar_function_set_function(fun.handle, ptr)
        fun
    end

end




# %% --- Conversions ------------------------------------------ #


function _udf_assign_result_init(::Type{T}, vec::Vec) where {T}
    T_internal = julia_to_duck_type(T)
    arr = get_array(vec, T_internal)  # this call is quite slow, so we only call it once
    return arr
end

function _udf_assign_result_init(::Type{T}, vec::Vec) where {T <: AbstractString}
    return nothing
end

function _udf_assign_result!(container, ::Type{T}, vec::Vec, result::T, index) where {T}
    container[index] = value_to_duckdb(result) # convert the value to duckdb and assign it to the array
    return nothing
end

function _udf_assign_result!(container, ::Type{T}, vec::Vec, result::T, index) where {T <: AbstractString}
    s = string(result)
    DuckDB.assign_string_element(vec, index, s)
    return nothing
end


function _udf_convert_chunk(::Type{T}, lt::LogicalType, chunk::DataChunk, ix) where {T <: Number}
    x::Vector{T} = get_array(chunk, ix, T)
    return x
end

function _udf_convert_chunk(::Type{T}, lt::LogicalType, chunk::DataChunk, ix) where {T <: AbstractString}
    data = ColumnConversionData((chunk,), ix, lt, nothing)
    return convert_column(data)
end

function _udf_convert_chunk(::Type{T}, lt::LogicalType, chunk::DataChunk, ix) where {T}
    data = ColumnConversionData((chunk,), ix, lt, nothing)
    return convert_column(data)
end

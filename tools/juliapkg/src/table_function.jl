#=
//===--------------------------------------------------------------------===//
// Table Function Bind
//===--------------------------------------------------------------------===//
=#
struct BindInfo
    handle::duckdb_bind_info
    main_function::Any

    function BindInfo(handle::duckdb_bind_info, main_function)
        result = new(handle, main_function)
        return result
    end
end

mutable struct InfoWrapper
    main_function::Any
    info::Any

    function InfoWrapper(main_function, info)
        return new(main_function, info)
    end
end

function parameter_count(bind_info::BindInfo)
    return duckdb_bind_get_parameter_count(bind_info.handle)
end

function get_parameter(bind_info::BindInfo, index::Int64)
    return Value(duckdb_bind_get_parameter(bind_info.handle, index))
end

function add_result_column(bind_info::BindInfo, name::AbstractString, type::DataType)
    return add_result_column(bind_info, name, create_logical_type(type))
end

function add_result_column(bind_info::BindInfo, name::AbstractString, type::LogicalType)
    return duckdb_bind_add_result_column(bind_info.handle, name, type.handle)
end

function get_extra_data(bind_info::BindInfo)
    return bind_info.main_function.extra_data
end

function _add_global_object(main_function, object)
    lock(main_function.global_lock)
    push!(main_function.global_objects, object)
    unlock(main_function.global_lock)
    return
end

function _remove_global_object(main_function, object)
    lock(main_function.global_lock)
    delete!(main_function.global_objects, object)
    unlock(main_function.global_lock)
    return
end

function _table_bind_cleanup(data::Ptr{Cvoid})
    info::InfoWrapper = unsafe_pointer_to_objref(data)
    _remove_global_object(info.main_function, info)
    return
end

function get_exception_info()
    error = ""
    if VERSION < v"1.7"
        for (exc, bt) in Base.catch_stack()
            error = string(error, sprint(showerror, exc, bt))
        end
    else
        for (exc, bt) in current_exceptions()
            error = string(error, sprint(showerror, exc, bt))
        end
    end
    return error
end

function _table_bind_function(info::duckdb_bind_info)
    try
        main_function = unsafe_pointer_to_objref(duckdb_bind_get_extra_info(info))
        binfo = BindInfo(info, main_function)
        bind_data = InfoWrapper(main_function, main_function.bind_func(binfo))
        bind_data_pointer = pointer_from_objref(bind_data)
        _add_global_object(main_function, bind_data)
        duckdb_bind_set_bind_data(info, bind_data_pointer, @cfunction(_table_bind_cleanup, Cvoid, (Ptr{Cvoid},)))
    catch
        duckdb_bind_set_error(info, get_exception_info())
        return
    end
    return
end

#=
//===--------------------------------------------------------------------===//
// Table Function Init
//===--------------------------------------------------------------------===//
=#
struct InitInfo
    handle::duckdb_init_info
    main_function::Any

    function InitInfo(handle::duckdb_init_info, main_function)
        result = new(handle, main_function)
        return result
    end
end

function _table_init_function_generic(info::duckdb_init_info, init_fun::Function)
    try
        main_function = unsafe_pointer_to_objref(duckdb_init_get_extra_info(info))
        binfo = InitInfo(info, main_function)
        init_data = InfoWrapper(main_function, init_fun(binfo))
        init_data_pointer = pointer_from_objref(init_data)
        _add_global_object(main_function, init_data)
        duckdb_init_set_init_data(info, init_data_pointer, @cfunction(_table_bind_cleanup, Cvoid, (Ptr{Cvoid},)))
    catch
        duckdb_init_set_error(info, get_exception_info())
        return
    end
    return
end

function _table_init_function(info::duckdb_init_info)
    main_function = unsafe_pointer_to_objref(duckdb_init_get_extra_info(info))
    return _table_init_function_generic(info, main_function.init_func)
end

function _table_local_init_function(info::duckdb_init_info)
    main_function = unsafe_pointer_to_objref(duckdb_init_get_extra_info(info))
    return _table_init_function_generic(info, main_function.init_local_func)
end

function get_bind_info(info::InitInfo, ::Type{T})::T where {T}
    return unsafe_pointer_to_objref(duckdb_init_get_bind_data(info.handle)).info
end

function get_extra_data(info::InitInfo)
    return info.main_function.extra_data
end

function set_max_threads(info::InitInfo, max_threads)
    return duckdb_init_set_max_threads(info.handle, max_threads)
end

function get_projected_columns(info::InitInfo)::Vector{Int64}
    result::Vector{Int64} = Vector()
    column_count = duckdb_init_get_column_count(info.handle)
    for i in 1:column_count
        push!(result, duckdb_init_get_column_index(info.handle, i))
    end
    return result
end

function _empty_init_info(info::DuckDB.InitInfo)
    return missing
end

#=
//===--------------------------------------------------------------------===//
// Main Table Function
//===--------------------------------------------------------------------===//
=#
struct FunctionInfo
    handle::duckdb_function_info
    main_function::Any

    function FunctionInfo(handle::duckdb_function_info, main_function)
        result = new(handle, main_function)
        return result
    end
end

function get_bind_info(info::FunctionInfo, ::Type{T})::T where {T}
    return unsafe_pointer_to_objref(duckdb_function_get_bind_data(info.handle)).info
end

function get_init_info(info::FunctionInfo, ::Type{T})::T where {T}
    return unsafe_pointer_to_objref(duckdb_function_get_init_data(info.handle)).info
end

function get_local_info(info::FunctionInfo, ::Type{T})::T where {T}
    return unsafe_pointer_to_objref(duckdb_function_get_local_init_data(info.handle)).info
end

function _table_main_function(info::duckdb_function_info, chunk::duckdb_data_chunk)
    main_function::TableFunction = unsafe_pointer_to_objref(duckdb_function_get_extra_info(info))
    binfo::FunctionInfo = FunctionInfo(info, main_function)
    try
        main_function.main_func(binfo, DataChunk(chunk, false))
    catch
        duckdb_function_set_error(info, get_exception_info())
    end
    return
end

#=
//===--------------------------------------------------------------------===//
// Table Function
//===--------------------------------------------------------------------===//
=#
"""
DuckDB table function
"""
mutable struct TableFunction
    handle::duckdb_table_function
    bind_func::Function
    init_func::Function
    init_local_func::Function
    main_func::Function
    extra_data::Any
    global_objects::Set{Any}
    global_lock::ReentrantLock

    function TableFunction(
        name::AbstractString,
        parameters::Vector{LogicalType},
        bind_func::Function,
        init_func::Function,
        init_local_func::Function,
        main_func::Function,
        extra_data::Any,
        projection_pushdown::Bool
    )
        handle = duckdb_create_table_function()
        duckdb_table_function_set_name(handle, name)
        for param in parameters
            duckdb_table_function_add_parameter(handle, param.handle)
        end
        result = new(handle, bind_func, init_func, init_local_func, main_func, extra_data, Set(), ReentrantLock())
        finalizer(_destroy_table_function, result)

        duckdb_table_function_set_extra_info(handle, pointer_from_objref(result))
        duckdb_table_function_set_bind(handle, @cfunction(_table_bind_function, Cvoid, (duckdb_bind_info,)))
        duckdb_table_function_set_init(handle, @cfunction(_table_init_function, Cvoid, (duckdb_init_info,)))
        duckdb_table_function_set_local_init(handle, @cfunction(_table_local_init_function, Cvoid, (duckdb_init_info,)))
        duckdb_table_function_set_function(
            handle,
            @cfunction(_table_main_function, Cvoid, (duckdb_function_info, duckdb_data_chunk))
        )
        duckdb_table_function_supports_projection_pushdown(handle, projection_pushdown)

        return result
    end
end

function _destroy_table_function(func::TableFunction)
    # disconnect from DB
    if func.handle != C_NULL
        duckdb_destroy_table_function(func.handle)
    end
    return func.handle = C_NULL
end

function create_table_function(
    con::Connection,
    name::AbstractString,
    parameters::Vector{LogicalType},
    bind_func::Function,
    init_func::Function,
    main_func::Function,
    extra_data::Any = missing,
    projection_pushdown::Bool = false,
    init_local_func::Union{Missing, Function} = missing
)
    if init_local_func === missing
        init_local_func = _empty_init_info
    end
    fun = TableFunction(
        name,
        parameters,
        bind_func,
        init_func,
        init_local_func,
        main_func,
        extra_data,
        projection_pushdown
    )
    if duckdb_register_table_function(con.handle, fun.handle) != DuckDBSuccess
        throw(QueryException(string("Failed to register table function \"", name, "\"")))
    end
    push!(con.db.functions, fun)
    return
end

function create_table_function(
    con::Connection,
    name::AbstractString,
    parameters::Vector{DataType},
    bind_func::Function,
    init_func::Function,
    main_func::Function,
    extra_data::Any = missing,
    projection_pushdown::Bool = false,
    init_local_func::Union{Missing, Function} = missing
)
    parameter_types::Vector{LogicalType} = Vector()
    for parameter_type in parameters
        push!(parameter_types, create_logical_type(parameter_type))
    end
    return create_table_function(
        con,
        name,
        parameter_types,
        bind_func,
        init_func,
        main_func,
        extra_data,
        projection_pushdown,
        init_local_func
    )
end

function create_table_function(
    db::DB,
    name::AbstractString,
    parameters::Vector{LogicalType},
    bind_func::Function,
    init_func::Function,
    main_func::Function,
    extra_data::Any = missing,
    projection_pushdown::Bool = false,
    init_local_func::Union{Missing, Function} = missing
)
    return create_table_function(
        db.main_connection,
        name,
        parameters,
        bind_func,
        init_func,
        main_func,
        extra_data,
        projection_pushdown,
        init_local_func
    )
end

function create_table_function(
    db::DB,
    name::AbstractString,
    parameters::Vector{DataType},
    bind_func::Function,
    init_func::Function,
    main_func::Function,
    extra_data::Any = missing,
    projection_pushdown::Bool = false,
    init_local_func::Union{Missing, Function} = missing
)
    return create_table_function(
        db.main_connection,
        name,
        parameters,
        bind_func,
        init_func,
        main_func,
        extra_data,
        projection_pushdown,
        init_local_func
    )
end

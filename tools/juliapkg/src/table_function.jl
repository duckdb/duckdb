#=
//===--------------------------------------------------------------------===//
// Table Function Bind
//===--------------------------------------------------------------------===//
=#
mutable struct BindInfo
    handle::duckdb_bind_info

    function BindInfo(handle::duckdb_bind_info)
        result = new(handle)
        return result
    end
end

function ParameterCount(bind_info::BindInfo)
    return duckdb_bind_get_parameter_count(bind_info.handle)
end

function GetParameter(bind_info::BindInfo, index::Int64)
    return Value(duckdb_bind_get_parameter(bind_info.handle, index))
end

function AddResultColumn(bind_info::BindInfo, name::AbstractString, type::LogicalType)
    return duckdb_bind_add_result_column(bind_info.handle, name, type.handle)
end

function _table_bind_function(info::duckdb_bind_info)
    main_info = unsafe_pointer_to_objref(duckdb_bind_get_extra_info(info))
    binfo = BindInfo(info)
    bind_data = main_info.bind_func(binfo)
    duckdb_bind_set_bind_data(info, pointer_from_objref(bind_data), C_NULL)
    return
end

#=
//===--------------------------------------------------------------------===//
// Table Function Init
//===--------------------------------------------------------------------===//
=#
mutable struct InitInfo
    handle::duckdb_init_info

    function InitInfo(handle::duckdb_init_info)
        result = new(handle)
        return result
    end
end

function _table_init_function(info::duckdb_init_info)
    main_info = unsafe_pointer_to_objref(duckdb_init_get_extra_info(info))
    binfo = InitInfo(info)
    init_data = main_info.init_func(binfo)
    duckdb_init_set_init_data(info, pointer_from_objref(init_data), C_NULL)
    return
end

#=
//===--------------------------------------------------------------------===//
// Main Table Function
//===--------------------------------------------------------------------===//
=#
mutable struct FunctionInfo
    handle::duckdb_function_info

    function FunctionInfo(handle::duckdb_function_info)
        result = new(handle)
        return result
    end
end

function GetBindInfo(info::FunctionInfo)
    return unsafe_pointer_to_objref(duckdb_function_get_bind_data(info.handle))
end

function GetInitInfo(info::FunctionInfo)
    return unsafe_pointer_to_objref(duckdb_function_get_init_data(info.handle))
end

function _table_main_function(info::duckdb_function_info, chunk::duckdb_data_chunk)
    main_info = unsafe_pointer_to_objref(duckdb_function_get_extra_info(info))
    binfo = FunctionInfo(info)
    return main_info.main_func(binfo, DataChunk(chunk))
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
    main_func::Function

    function TableFunction(
        name::AbstractString,
        parameters::Vector{LogicalType},
        bind_func::Function,
        init_func::Function,
        main_func::Function
    )
        handle = duckdb_create_table_function()
        duckdb_table_function_set_name(handle, name)
        for param in parameters
            duckdb_table_function_add_parameter(handle, param.handle)
        end
        result = new(handle, bind_func, init_func, main_func)
        finalizer(_destroy_table_function, result)

        duckdb_table_function_set_extra_info(handle, pointer_from_objref(result))
        duckdb_table_function_set_bind(handle, @cfunction(_table_bind_function, Cvoid, (duckdb_bind_info,)))
        duckdb_table_function_set_init(handle, @cfunction(_table_init_function, Cvoid, (duckdb_init_info,)))
        duckdb_table_function_set_function(
            handle,
            @cfunction(_table_main_function, Cvoid, (duckdb_function_info, duckdb_data_chunk))
        )

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

function CreateTableFunction(
    con::Connection,
    name::AbstractString,
    parameters::Vector{LogicalType},
    bind_func::Function,
    init_func::Function,
    main_func::Function
)
    fun = TableFunction(name, parameters, bind_func, init_func, main_func)
    if duckdb_register_table_function(con.handle, fun.handle) != DuckDBSuccess
        throw(QueryException(string("Failed to register table function \"", name, "\"")))
    end
    return push!(con.db.functions, fun)
end

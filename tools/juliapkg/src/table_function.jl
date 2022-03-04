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

function ParameterCount(bind_info::BindInfo)
    return duckdb_bind_get_parameter_count(bind_info.handle)
end

function GetParameter(bind_info::BindInfo, index::Int64)
    return Value(duckdb_bind_get_parameter(bind_info.handle, index))
end

function AddResultColumn(bind_info::BindInfo, name::AbstractString, type::DataType)
	AddResultColumn(bind_info, name, CreateLogicalType(type))
end

function AddResultColumn(bind_info::BindInfo, name::AbstractString, type::LogicalType)
    return duckdb_bind_add_result_column(bind_info.handle, name, type.handle)
end

function GetExtraData(bind_info::BindInfo)
    return bind_info.main_function.extra_data
end

function _table_bind_function(info::duckdb_bind_info)
    main_function = unsafe_pointer_to_objref(duckdb_bind_get_extra_info(info))
    binfo = BindInfo(info, main_function)
    bind_data = main_function.bind_func(binfo)
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
    main_function::Any

    function InitInfo(handle::duckdb_init_info, main_function)
        result = new(handle, main_function)
        return result
    end
end

function _table_init_function(info::duckdb_init_info)
    main_function = unsafe_pointer_to_objref(duckdb_init_get_extra_info(info))
    binfo = InitInfo(info, main_function)
    init_data = main_function.init_func(binfo)
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
    main_function::Any

    function FunctionInfo(handle::duckdb_function_info, main_function)
        result = new(handle, main_function)
        return result
    end
end

function GetBindInfo(info::FunctionInfo, ::Type{T})::T where {T}
    return unsafe_pointer_to_objref(duckdb_function_get_bind_data(info.handle))
end

function GetInitInfo(info::FunctionInfo, ::Type{T})::T where {T}
    return unsafe_pointer_to_objref(duckdb_function_get_init_data(info.handle))
end

function _table_main_function(info::duckdb_function_info, chunk::duckdb_data_chunk)
    main_function = unsafe_pointer_to_objref(duckdb_function_get_extra_info(info))
    binfo = FunctionInfo(info, main_function)
    main_function.main_func(binfo, DataChunk(chunk))
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
    main_func::Function
    extra_data::Any

    function TableFunction(
        name::AbstractString,
        parameters::Vector{LogicalType},
        bind_func::Function,
        init_func::Function,
        main_func::Function,
        extra_data::Any
    )
        handle = duckdb_create_table_function()
        duckdb_table_function_set_name(handle, name)
        for param in parameters
            duckdb_table_function_add_parameter(handle, param.handle)
        end
        result = new(handle, bind_func, init_func, main_func, extra_data)
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
    main_func::Function,
    extra_data::Any = missing
)
    fun = TableFunction(name, parameters, bind_func, init_func, main_func, extra_data)
    if duckdb_register_table_function(con.handle, fun.handle) != DuckDBSuccess
        throw(QueryException(string("Failed to register table function \"", name, "\"")))
    end
    push!(con.db.functions, fun)
    return
end

function CreateTableFunction(
    con::Connection,
    name::AbstractString,
    parameters::Vector{DataType},
    bind_func::Function,
    init_func::Function,
    main_func::Function,
    extra_data::Any = missing
)
	parameter_types::Vector{LogicalType} = Vector()
	for parameter_type in parameters
		push!(parameter_types, CreateLogicalType(parameter_type))
	end
	CreateTableFunction(con, name, parameter_types, bind_func, init_func, main_func, extra_data)
end

function CreateTableFunction(
    db::DB,
    name::AbstractString,
    parameters::Vector{LogicalType},
    bind_func::Function,
    init_func::Function,
    main_func::Function,
    extra_data::Any = missing
)
    return CreateTableFunction(db.main_connection, name, parameters, bind_func, init_func, main_func)
end

function CreateTableFunction(
    db::DB,
    name::AbstractString,
    parameters::Vector{DataType},
    bind_func::Function,
    init_func::Function,
    main_func::Function,
    extra_data::Any = missing
)
    return CreateTableFunction(db.main_connection, name, parameters, bind_func, init_func, main_func)
end

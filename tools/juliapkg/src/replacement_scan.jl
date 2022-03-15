

# function duckdb_add_replacement_scan(db, replacement, extra_data, delete_callback)
#
# function duckdb_replacement_scan_set_function_name(info, function_name)
#
# function duckdb_replacement_scan_add_parameter(info, parameter)

mutable struct ReplacementFunction
	db::DB
	replacement_func::Function
	extra_data::Any
end

struct ReplacementFunctionInfo
    handle::duckdb_replacement_scan_info
    main_function::ReplacementFunction
    table_name::String

    function ReplacementFunctionInfo(handle::duckdb_replacement_scan_info, main_function::ReplacementFunction, table_name::String)
        result = new(handle, main_function, table_name)
        return result
    end
end

function _replacement_scan_function(handle::duckdb_replacement_scan_info, table_name::Ptr{UInt8}, data::Ptr{Cvoid})
    func::ReplacementFunction = unsafe_pointer_to_objref(data)
    tname = unsafe_string(table_name)
    info = ReplacementFunctionInfo(handle, func, tname)
    func.replacement_func(info)
    return
end

function GetDB(info::ReplacementFunctionInfo)
	return info.main_function.db
end

function GetExtraData(info::ReplacementFunctionInfo)
	return info.main_function.extra_data
end

function GetTableName(info::ReplacementFunctionInfo)
	return info.table_name
end

function SetFunctionName(info::ReplacementFunctionInfo, function_name::String)
	duckdb_replacement_scan_set_function_name(info.handle, function_name)
end

function AddFunctionParameter(info::ReplacementFunctionInfo, parameter::Value)
	duckdb_replacement_scan_add_parameter(info.handle, parameter.handle)
end

function _replacement_func_cleanup(data::Ptr{Cvoid})
    info::ReplacementFunction = unsafe_pointer_to_objref(data)
    delete!(global_objects, info)
    return
end

function AddReplacementScan(
    db::DB,
    replacement_func::Function,
    extra_data::Any
)
	func = ReplacementFunction(db, replacement_func, extra_data)
	push!(global_objects, func)
	duckdb_add_replacement_scan(
		db.handle.handle,
		@cfunction(_replacement_scan_function, Cvoid, (duckdb_replacement_scan_info,Ptr{UInt8},Ptr{Cvoid})),
		pointer_from_objref(func),
		@cfunction(_replacement_func_cleanup, Cvoid, (Ptr{Cvoid},))
	)
end

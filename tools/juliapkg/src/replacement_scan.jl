

mutable struct ReplacementFunction
    db::DB
    replacement_func::Function
    extra_data::Any
    uuid::UUID
end

struct ReplacementFunctionInfo
    handle::duckdb_replacement_scan_info
    main_function::ReplacementFunction
    table_name::String

    function ReplacementFunctionInfo(
        handle::duckdb_replacement_scan_info,
        main_function::ReplacementFunction,
        table_name::String
    )
        result = new(handle, main_function, table_name)
        return result
    end
end

function _replacement_scan_function(handle::duckdb_replacement_scan_info, table_name::Ptr{UInt8}, data::Ptr{Cvoid})
    try
        func::ReplacementFunction = unsafe_pointer_to_objref(data)
        tname = unsafe_string(table_name)
        info = ReplacementFunctionInfo(handle, func, tname)
        func.replacement_func(info)
    catch
        duckdb_replacement_scan_set_error(handle, get_exception_info())
        return
    end
end

function getdb(info::ReplacementFunctionInfo)
    return info.main_function.db
end

function get_extra_data(info::ReplacementFunctionInfo)
    return info.main_function.extra_data
end

function get_table_name(info::ReplacementFunctionInfo)
    return info.table_name
end

function set_function_name(info::ReplacementFunctionInfo, function_name::String)
    return duckdb_replacement_scan_set_function_name(info.handle, function_name)
end

function add_function_parameter(info::ReplacementFunctionInfo, parameter::Value)
    return duckdb_replacement_scan_add_parameter(info.handle, parameter.handle)
end

function _replacement_func_cleanup(data::Ptr{Cvoid})
    info::ReplacementFunction = unsafe_pointer_to_objref(data)
    delete!(info.db.handle.registered_objects, info.uuid)
    return
end

function add_replacement_scan!(db::DB, replacement_func::Function, extra_data::Any)
    func = ReplacementFunction(db, replacement_func, extra_data, uuid4())
    db.handle.registered_objects[func.uuid] = func
    return duckdb_add_replacement_scan(
        db.handle.handle,
        @cfunction(_replacement_scan_function, Cvoid, (duckdb_replacement_scan_info, Ptr{UInt8}, Ptr{Cvoid})),
        pointer_from_objref(func),
        @cfunction(_replacement_func_cleanup, Cvoid, (Ptr{Cvoid},))
    )
end

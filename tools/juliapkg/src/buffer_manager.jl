# Should we add defined Function types for each of these callbacks?
function add_custom_buffer_manager!(
	config::Config,
	extra_data::Any,
	allocate_func::Function,
	reallocate_func::Function,
	destroy_func::Function,
	get_allocation_func::Function,
	pin_func::Function,
	unpin_func::Function,
	used_memory_func::Function,
	max_memory_func::Function
)
	# Do we want to add the 'extra_data' to the config, to secure it from GC?
    #config.handle.registered_objects[func.uuid] = func
    return duckdb_add_custom_buffer_manager(
        config.handle.handle,
        pointer_from_objref(extra_data),
        @cfunction(allocate_func, duckdb_buffer, (Ptr{Cvoid}, Int32)),
        @cfunction(reallocate_func, duckdb_buffer, (duckdb_buffer, Int32, Int32)),
        @cfunction(destroy_func, Cvoid, (duckdb_buffer)),
        @cfunction(get_allocation_func, Ptr{Cvoid}, (duckdb_buffer)),
        @cfunction(pin_func, Cvoid, (duckdb_buffer)),
        @cfunction(unpin_func, Cvoid, (duckdb_buffer)),
        @cfunction(used_memory_func, Int32, (Ptr{Cvoid})),
        @cfunction(max_memory_func, Int32, (Ptr{Cvoid})),
    )
end

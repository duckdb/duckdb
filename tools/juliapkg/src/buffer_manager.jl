mutable struct AllocateResult
	value::duckdb_buffer
end
mutable struct ReAllocateResult
	value::duckdb_buffer
end
mutable struct PinResult
	value::Ptr{Cvoid}
end
mutable struct UsedMemoryResult
	value::Int64
end
mutable struct MaxMemoryResult
	value::Int64
end

# These functions are provided as function pointers to the c_api, and stored in the CBufferManager config
# They act as wrappers around the user-provided functions
# the function gets called and if any exception occurs the error is registered

# They return a boolean indicating whether the execution was successful, and set the `result` variable if successful

struct CBufferManagerData
	# User provided data
	extra_data::Ptr{Cvoid}

	# User provided callbacks
	allocate::duckdb_cbuffermanager_allocate
	reallocate::duckdb_cbuffermanager_reallocate
	destroy::duckdb_cbuffermanager_destroy
	pin::duckdb_cbuffermanager_pin
	unpin::duckdb_cbuffermanager_unpin
	used_memory::duckdb_cbuffermanager_used_memory
	max_memory::duckdb_cbuffermanager_max_memory

	# Identify this object
	uuid::UUID
end

function _allocate(handle, buffer_manager::Ptr{Cvoid}, allocation_size::Int32, result::{Ptr{Cvoid}})
	try
		res::AllocateResult = unsafe_pointer_to_objref(result)
		res.value = handle.allocate(buffer_manager, allocation_size)
	catch
		duckdb_custom_buffer_manager_set_error(handle, get_exception_info())
		return false
	end
	return true
end

function _reallocate(handle, buffer::duckdb_buffer, old_size::Int32, new_size::Int32, result::{Ptr{Cvoid}})
	try
		res::ReAllocateResult = unsafe_pointer_to_objref(result)
		res.value = handle.reallocate(buffer, old_size, new_size)
	catch
		duckdb_custom_buffer_manager_set_error(handle, get_exception_info())
		return false
	end
	return true
end

function _destroy(handle, buffer::duckdb_buffer)
	try
		handle.destroy(buffer)
	catch
		duckdb_custom_buffer_manager_set_error(handle, get_exception_info())
		return false
	end
	return true
end

function _pin(handle, buffer::duckdb_buffer, result::Ptr{Cvoid})
	try
		res::PinResult = unsafe_pointer_to_objref(result)
		res.value = handle.pin(buffer)
	catch
		duckdb_custom_buffer_manager_set_error(handle, get_exception_info())
		return false
	end
	return true
end

function _unpin(handle, buffer::duckdb_buffer)
	try
		handle.unpin(buffer)
	catch
		duckdb_custom_buffer_manager_set_error(handle, get_exception_info())
		return false
	end
	return true
end

function _used_memory(handle, buffer_manager::Ptr{Cvoid}, result::Ptr{Cvoid})
	try
		res::UsedMemoryResult = unsafe_pointer_to_objref(result)
		res.value = handle.used_memory(buffer_manager)
	catch
		duckdb_custom_buffer_manager_set_error(handle, get_exception_info())
		return false
	end
	return true
end

function _max_memory(handle, buffer_manager::Ptr{Cvoid}, result::Ptr{Cvoid})
	try
		res::MaxMemoryResult = unsafe_pointer_to_objref(result)
		res.value = handle.max_memory(buffer_manager)
	catch
		duckdb_custom_buffer_manager_set_error(handle, get_exception_info())
		return false
	end
	return true
end

# Should we add defined Function types for each of these callbacks?
function add_custom_buffer_manager!(
    config::Config,
    extra_data::Any,
    allocate_func::Function,
    reallocate_func::Function,
    destroy_func::Function,
    pin_func::Function,
    unpin_func::Function,
    used_memory_func::Function,
    max_memory_func::Function
)
    # Do we want to add the 'extra_data' to the config, to secure it from GC?
    #config.handle.registered_objects[func.uuid] = func

	# TODO: wrap the 'extra_data' in another object bundled with the user-provided functions
	# Then we can unwrap this in our functions

    return duckdb_add_custom_buffer_manager(
        config.handle,
        pointer_from_objref(extra_data),
        @cfunction(_allocate, duckdb_buffer, (Ptr{Cvoid}, Int32)),
        @cfunction(_reallocate, duckdb_buffer, (duckdb_buffer, Int32, Int32)),
        @cfunction(_destroy, Cvoid, (duckdb_buffer,)),
        @cfunction(_pin, Cvoid, (duckdb_buffer,)),
        @cfunction(_unpin, Cvoid, (duckdb_buffer,)),
        @cfunction(_used_memory, Int32, (Ptr{Cvoid},)),
        @cfunction(_max_memory, Int32, (Ptr{Cvoid},))
    )
end

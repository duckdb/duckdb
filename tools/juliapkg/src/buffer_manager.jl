
# TODO: make these function prototypes more strict
mutable struct CBufferManagerData
    # User provided data
    extra_data::Any

    # User provided callbacks
    allocate::Function
    reallocate::Function
    destroy::Function
    pin::Function
    unpin::Function
    max_memory::Function
    used_memory::Function

    # Identify this object
    uuid::UUID
end

# TODO: these functions need exception handling, the C versions also don't provide a way to indicate an error

function _allocate(wrapped_data::Ptr{Cvoid}, allocation_size::Int32)
	try
		data::CBufferManagerData = unsafe_pointer_to_objref(wrapped_data)
		buffer = data.allocate(data.extra_data, allocation_size)
		return pointer_from_objref(buffer)
	catch
		print("ALLOCATE FAILED", get_exception_info())
	end
	return
end

function _reallocate(wrapped_data::Ptr{Cvoid}, buffer::Any, old_size::Int32, new_size::Int32)
	try
		data::CBufferManagerData = unsafe_pointer_to_objref(wrapped_data)
		julia_buffer::Any = unsafe_pointer_to_objref(buffer)
		new_buffer = data.reallocate(data.extra_data, julia_buffer, old_size, new_size)
		return pointer_from_objref(new_buffer)
	catch
		print("REALLOCATE FAILED", get_exception_info())
	end
	return
end

function _destroy(wrapped_data::Ptr{Cvoid}, buffer::Any)
	try
		julia_buffer::Any = unsafe_pointer_to_objref(buffer)
		data::CBufferManagerData = unsafe_pointer_to_objref(wrapped_data)
		data.destroy(data.extra_data, julia_buffer)
		return
	catch
		print("DESTROY FAILED", get_exception_info())
	end
	return
end

function _pin(wrapped_data::Ptr{Cvoid}, buffer::Any)
	try
		julia_buffer::Any = unsafe_pointer_to_objref(buffer)
		data::CBufferManagerData = unsafe_pointer_to_objref(wrapped_data)
		return data.pin(data.extra_data, julia_buffer)
	catch
		print("PIN FAILED", get_exception_info())
	end
	return
end

function _unpin(wrapped_data::Ptr{Cvoid}, buffer::Any)
	try
		julia_buffer::Any = unsafe_pointer_to_objref(buffer)
		data::CBufferManagerData = unsafe_pointer_to_objref(wrapped_data)
		data.unpin(data.extra_data, julia_buffer)
		return
	catch
		print("UNPIN FAILED", get_exception_info())
	end
	return
end

function _used_memory(wrapped_data::Ptr{Cvoid})
	try
		data::CBufferManagerData = unsafe_pointer_to_objref(wrapped_data)
		return data.used_memory(data.extra_data)
	catch
		print("USED MEMORY FAILED", get_exception_info())
	end
	return
end

function _max_memory(wrapped_data::Ptr{Cvoid})::Cint
	try
		data::CBufferManagerData = unsafe_pointer_to_objref(wrapped_data)
		return data.max_memory(data.extra_data)
	catch
		print("MAX MEMORY FAILED", get_exception_info())
	end
	return 0 #FIXME
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
    max_memory_func::Function,
    used_memory_func::Function
)

    wrapped_data = CBufferManagerData(
        extra_data,
        allocate_func,
        reallocate_func,
        destroy_func,
        pin_func,
        unpin_func,
        max_memory_func,
        used_memory_func,
        uuid4()
    )

    #FIXME: do we need a delete_callback like 'replacement_scan' has?

    config.registered_objects[wrapped_data.uuid] = wrapped_data
    return duckdb_add_custom_buffer_manager(
        config.handle,
        pointer_from_objref(wrapped_data),
        @cfunction(_allocate, duckdb_buffer, (Ptr{Cvoid}, Int32)),
        @cfunction(_reallocate, duckdb_buffer, (Ptr{Cvoid}, duckdb_buffer, Int32, Int32)),
        @cfunction(_destroy, Cvoid, (Ptr{Cvoid}, duckdb_buffer)),
        @cfunction(_pin, Ptr{Cvoid}, (Ptr{Cvoid}, duckdb_buffer)),
        @cfunction(_unpin, Cvoid, (Ptr{Cvoid}, duckdb_buffer)),
        @cfunction(_max_memory, Int32, (Ptr{Cvoid},)),
        @cfunction(_used_memory, Int32, (Ptr{Cvoid},))
    )
end

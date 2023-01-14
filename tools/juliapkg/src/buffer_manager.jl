mutable struct CBufferManagerData
    # User provided data
    extra_data::Any

    # User provided callbacks
    allocate::Function
    reallocate::Function
    destroy::Function
    pin::Function
    unpin::Function
    used_memory::Function
    max_memory::Function

    # Identify this object
    uuid::UUID
end

# TODO: these functions need exception handling, the C versions also don't provide a way to indicate an error

function _allocate(wrapped_data, allocation_size::Int32)
    data::CBufferManagerData = unsafe_pointer_to_objref(wrapped_data)
    return data.allocate(data.extra_data, allocation_size)
end

function _reallocate(wrapped_data, buffer::duckdb_buffer, old_size::Int32, new_size::Int32)
    data::CBufferManagerData = unsafe_pointer_to_objref(wrapped_data)
    return data.reallocate(data.extra_data, buffer, old_size, new_size)
end

function _destroy(wrapped_data, buffer::duckdb_buffer)
    data::CBufferManagerData = unsafe_pointer_to_objref(wrapped_data)
    return data.destroy(data.extra_data, buffer)
end

function _pin(wrapped_data, buffer::duckdb_buffer)
    data::CBufferManagerData = unsafe_pointer_to_objref(wrapped_data)
    return data.pin(data.extra_data, buffer)
end

function _unpin(wrapped_data, buffer::duckdb_buffer)
    data::CBufferManagerData = unsafe_pointer_to_objref(wrapped_data)
    return data.unpin(data.extra_data, buffer)
end

function _used_memory(wrapped_data)
    data::CBufferManagerData = unsafe_pointer_to_objref(wrapped_data)
    return data.used_memory(data.extra_data)
end

function _max_memory(wrapped_data)
    data::CBufferManagerData = unsafe_pointer_to_objref(wrapped_data)
    return data.max_memory(data.extra_data)
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

    wrapped_data = CBufferManagerData(
        extra_data,
        allocate_func,
        reallocate_func,
        destroy_func,
        pin_func,
        unpin_func,
        used_memory_func,
        max_memory_func,
        uuid4()
    )

    #FIXME: do we need a delete_callback like 'replacement_scan' has?

    config.registered_objects[wrapped_data.uuid] = wrapped_data
    return duckdb_add_custom_buffer_manager(
        config.handle,
        pointer_from_objref(wrapped_data),
        @cfunction(_allocate, duckdb_buffer, (Ptr{Cvoid}, Int32)),
        @cfunction(_reallocate, duckdb_buffer, (duckdb_buffer, Int32, Int32)),
        @cfunction(_destroy, Cvoid, (duckdb_buffer,)),
        @cfunction(_pin, Cvoid, (duckdb_buffer,)),
        @cfunction(_unpin, Cvoid, (duckdb_buffer,)),
        @cfunction(_used_memory, Int32, (Ptr{Cvoid},)),
        @cfunction(_max_memory, Int32, (Ptr{Cvoid},))
    )
end

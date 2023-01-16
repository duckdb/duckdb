# test_tpch.jl

# 10mb
const DEFAULT_MAX_MEMORY = 10000000

mutable struct MyBufferManager
    allocated_memory::Int32
    pinned_buffers::Int32
    max_memory::Int32
    active_buffers::Dict{Any, Any}

    # Create a buffer manager with a specific max_memory
    function MyBufferManager(max_memory::Int32)
        buffer_manager = new(0, 0, max_memory, Dict())
        return buffer_manager
    end

    # Create a buffer manager using the default max_memory limit
    function MyBufferManager()
        buffer_manager = new(0, 0, DEFAULT_MAX_MEMORY, Dict())
        return buffer_manager
    end
end

function allocate_memory(buffer_manager::MyBufferManager, size::Int32)
    allocation = DuckDB.duckdb_malloc(size)
    buffer_manager.allocated_memory += size
    return allocation
end

mutable struct MyBuffer
    buffer_manager::MyBufferManager
    allocation::Ptr{Cvoid}
    size::Int32
    pinned::Int32
    uuid::UUID

    # Create a buffer initialized with allocation+size in an unpinned state
    function MyBuffer(buffer_manager::MyBufferManager, size::Int32)
        allocation = allocate_memory(buffer_manager, size)
        buffer = new(buffer_manager, allocation, size, 0, uuid4())
        finalizer(remove_buffer, buffer)
        buffer_manager.active_buffers[buffer.uuid] = buffer
        return buffer
    end
end

# Clean up the memory allocated by the buffer
function remove_buffer(buffer::MyBuffer)
    DuckDB.duckdb_free(buffer.allocation)
end

function reallocate_memory(buffer::MyBuffer, new_size)
    buffer_manager = buffer.buffer_manager
    buffer_manager.allocated_memory -= buffer.size
    buffer_manager.allocated_memory = new_size
    buffer.allocation = DuckDB.duckdb_realloc(buffer.allocation, new_size)
    return buffer
end

# FIXME: Who is responsible for not letting this MyBuffer go out of scope?
function Allocate(buffer_manager::MyBufferManager, allocation_size)
    # Use the provided buffer manager to allocate a block of 'allocation_size' bytes
    return MyBuffer(buffer_manager, allocation_size)
end

function ReAllocate(buffer_manager::MyBufferManager, buffer::MyBuffer, old_size, new_size)
    # old_size is already stored in the buffer
    return reallocate_memory!(buffer, new_size)
end

function Destroy(buffer_manager::MyBufferManager, buffer::MyBuffer)
    buffer_manager.allocated_memory -= buffer.size
	# Remove the reference to the buffer so it can be cleaned up by the garbage collector
	pop!(buffer_manager.active_buffers, buffer.uuid)
    return
end

function Pin(buffer_manager::MyBufferManager, buffer::MyBuffer)
    if (buffer.pinned == 0)
        buffer_manager.pinned_buffers += 1
    end
    buffer.pinned += 1
    return buffer.allocation
end

function Unpin(buffer_manager::MyBufferManager, buffer::MyBuffer)
    buffer.pinned -= 1
    if (buffer.pinned == 0)
        buffer_manager.pinned_buffers -= 1
    end
    return
end

function MaxMemory(buffer_manager::MyBufferManager)
    return buffer_manager.max_memory
end

function UsedMemory(buffer_manager::MyBufferManager)
    return buffer_manager.allocated_memory
end

@testset "Test BufferManager" begin

    # Create a config
    config = DuckDB.Config()
    # Create a custom buffer manager
    buffer_manager = MyBufferManager()
    # Assign our custom buffer manager to it
    DuckDB.add_custom_buffer_manager!(
        config,
        buffer_manager,
        Allocate,
        ReAllocate,
        Destroy,
        Pin,
        Unpin,
        MaxMemory,
        UsedMemory
    )

    # Start a database with this config
    database = DuckDB.DB(":memory:", config)
    # Create a connection to this database
    con = DBInterface.connect(database)

    # Perform queries using it
    sf = "0.01"

    # load TPC-H into DuckDB
    DBInterface.execute(con, "CALL dbgen(sf=$sf)")

    # run all the queries
    for i in 1:22
        res = DataFrame(DBInterface.execute(con, "PRAGMA tpch($i)"))
    end

    DBInterface.close!(con)
end

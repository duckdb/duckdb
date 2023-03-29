#include "duckdb/storage/cbuffer_manager.hpp"
#include "duckdb/storage/cbuffer_manager_default.hpp"

using namespace duckdb;
using namespace std;

static MyBuffer *GetBuffer(duckdb_block buffer) {
	return (MyBuffer *)(buffer);
}

MyBuffer *CreateBuffer(void *allocation, idx_t size, MyBufferManager *buffer_manager) {
	auto malloc_result = malloc(sizeof(MyBuffer));
	MyBuffer *buffer = GetBuffer(malloc_result);
	if (!malloc_result) {
		free(allocation);
		throw IOException("Could not allocate %d bytes", sizeof(MyBuffer));
	}

	buffer->size = size;
	buffer->pinned = 0;
	buffer->allocation = allocation;
	buffer->buffer_manager = buffer_manager;
	buffer_manager->allocated_memory += size;
#ifdef DEBUG
	{
		lock_guard<mutex> lock(buffer_manager->lock);
		buffer_manager->allocated_buffers.insert((data_ptr_t)buffer);
	}
#endif
	return buffer;
}

duckdb_block Allocate(void *data, idx_t size) {
	auto my_data = (MyBufferManager *)data;
	void *allocation = malloc(size);
	if (!allocation) {
		throw IOException("Could not allocate %d bytes", size);
	}
	auto buffer = CreateBuffer(allocation, size, my_data);
	return buffer;
}

void Destroy(void *data, duckdb_block buffer) {
	(void)data;
	auto my_buffer = GetBuffer(buffer);
	auto buffer_manager = my_buffer->buffer_manager;
	//#ifdef DEBUG
	//	// This indicates a double-free
	//	D_ASSERT(buffer_manager->allocated_buffers.count((data_ptr_t)my_buffer));
	//#endif

	// assert that the buffer was not pinned, otherwise it should not be allowed to be destroyed
	D_ASSERT(my_buffer->pinned <= 1);

	free(my_buffer->allocation);
	buffer_manager->allocated_memory -= my_buffer->size;
#ifdef DEBUG
	{
		lock_guard<mutex> lock(buffer_manager->lock);
		buffer_manager->allocated_buffers.erase((data_ptr_t)my_buffer);
		buffer_manager->freed_buffers.insert((data_ptr_t)my_buffer);
	}
#endif
	free(buffer);
}

duckdb_block ReAllocate(void *data, duckdb_block buffer, idx_t old_size, idx_t new_size) {
	(void)data;
	auto my_buffer = GetBuffer(buffer);
	auto buffer_manager = my_buffer->buffer_manager;

#ifdef DEBUG
	{
		lock_guard<mutex> lock(buffer_manager->lock);
		// This indicates a heap-use-after-free
		D_ASSERT(!buffer_manager->freed_buffers.count((data_ptr_t)buffer));
		// Has to be allocated
		D_ASSERT(buffer_manager->allocated_buffers.count((data_ptr_t)buffer));
	}
#endif

	Destroy(data, buffer);
	return Allocate(buffer_manager, new_size);
}

void *Pin(void *data, duckdb_block buffer) {
	(void)data;
	auto my_buffer = (MyBuffer *)buffer;
	auto buffer_manager = my_buffer->buffer_manager;
#ifdef DEBUG
	{
		lock_guard<mutex> lock(buffer_manager->lock);
		// this doesn't really work.. but at least it will segfault if the pointer is faulty
		D_ASSERT(buffer_manager->allocated_buffers.count((data_ptr_t)buffer));
		if (my_buffer->pinned == 0) {
			buffer_manager->pinned_buffers.insert((data_ptr_t)my_buffer);
		}
	}
#endif

	my_buffer->pinned++;
	return my_buffer->allocation;
}

void Unpin(void *data, duckdb_block buffer) {
	(void)data;
	auto my_buffer = (MyBuffer *)buffer;
	auto buffer_manager = my_buffer->buffer_manager;
#ifdef DEBUG
	{
		lock_guard<mutex> lock(buffer_manager->lock);
		D_ASSERT(buffer_manager->allocated_buffers.count((data_ptr_t)buffer));
	}
#endif

	// assert that the buffer was pinnned
	D_ASSERT(my_buffer->pinned > 0);

	my_buffer->pinned--;
#ifdef DEBUG
	{
		lock_guard<mutex> lock(buffer_manager->lock);
		if (my_buffer->pinned == 0) {
			D_ASSERT(buffer_manager->pinned_buffers.size() > 0);
			buffer_manager->pinned_buffers.erase((data_ptr_t)my_buffer);
		}
	}
#endif
}

idx_t UsedMemory(void *data) {
	auto my_data = (MyBufferManager *)data;

	return my_data->allocated_memory;
}

idx_t MaxMemory(void *data) {
	auto my_data = (MyBufferManager *)data;

	return my_data->max_memory;
}

namespace duckdb {

duckdb::CBufferManagerConfig DefaultCBufferManagerConfig(MyBufferManager *manager) {
	duckdb::CBufferManagerConfig cbuffer_manager_config;

	cbuffer_manager_config.data = manager;
	cbuffer_manager_config.allocate_func = Allocate;
	cbuffer_manager_config.reallocate_func = ReAllocate;
	cbuffer_manager_config.destroy_func = Destroy;
	cbuffer_manager_config.pin_func = Pin;
	cbuffer_manager_config.unpin_func = Unpin;
	cbuffer_manager_config.max_memory_func = MaxMemory;
	cbuffer_manager_config.used_memory_func = UsedMemory;
	return cbuffer_manager_config;
}

} // namespace duckdb

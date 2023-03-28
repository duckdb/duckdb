#include "duckdb/storage/cbuffer_manager.hpp"
#include "duckdb/storage/cbuffer_manager_default.hpp"

using namespace duckdb;
using namespace std;

static MyBuffer *GetBuffer(duckdb_buffer buffer, idx_t header_bytes) {
	return (MyBuffer *)((int8_t *)buffer + header_bytes);
}

MyBuffer *CreateBuffer(void *allocation, idx_t size, MyBufferManager *buffer_manager, idx_t header_bytes) {
	auto malloc_result = malloc(sizeof(MyBuffer) + header_bytes);
	MyBuffer *buffer = GetBuffer(malloc_result, header_bytes);
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
	buffer_manager->allocated_buffers.insert((data_ptr_t)buffer);
#endif
	return buffer;
}

duckdb_buffer Allocate(void *data, idx_t size, idx_t header_bytes) {
	auto my_data = (MyBufferManager *)data;
	void *allocation = malloc(size);
	if (!allocation) {
		throw IOException("Could not allocate %d bytes", size);
	}
	auto buffer = CreateBuffer(allocation, size, my_data, header_bytes);
	return (int8_t *)buffer - header_bytes;
}

void Destroy(void *data, duckdb_buffer buffer, idx_t header_bytes) {
	(void)data;
	auto my_buffer = GetBuffer(buffer, header_bytes);
	auto buffer_manager = my_buffer->buffer_manager;
	//#ifdef DEBUG
	//	// This indicates a double-free
	//	D_ASSERT(buffer_manager->allocated_buffers.count((data_ptr_t)my_buffer));
	//#endif

	// assert that the buffer was not pinned, otherwise it should not be allowed to be destroyed
	D_ASSERT(my_buffer->pinned == 0);

	free(my_buffer->allocation);
	buffer_manager->allocated_memory -= my_buffer->size;
#ifdef DEBUG
	buffer_manager->allocated_buffers.erase((data_ptr_t)my_buffer);
	buffer_manager->freed_buffers.insert((data_ptr_t)my_buffer);
#endif
	free(buffer);
}

duckdb_buffer ReAllocate(void *data, duckdb_buffer buffer, idx_t old_size, idx_t new_size, idx_t header_bytes) {
	(void)data;
	auto my_buffer = GetBuffer(buffer, header_bytes);
	auto buffer_manager = my_buffer->buffer_manager;

#ifdef DEBUG
	// This indicates a heap-use-after-free
	D_ASSERT(!buffer_manager->freed_buffers.count((data_ptr_t)buffer));
	// Has to be allocated
	D_ASSERT(buffer_manager->allocated_buffers.count((data_ptr_t)buffer));
#endif

	Destroy(data, buffer, header_bytes);
	return Allocate(buffer_manager, new_size, header_bytes);
}

void *Pin(void *data, duckdb_buffer buffer) {
	(void)data;
	auto my_buffer = (MyBuffer *)buffer;
	auto buffer_manager = my_buffer->buffer_manager;
#ifdef DEBUG
	// this doesn't really work.. but at least it will segfault if the pointer is faulty
	D_ASSERT(buffer_manager->allocated_buffers.count((data_ptr_t)buffer));
#endif

	if (my_buffer->pinned == 0) {
		buffer_manager->pinned_buffers++;
	}
	my_buffer->pinned++;
	return my_buffer->allocation;
}

void Unpin(void *data, duckdb_buffer buffer) {
	(void)data;
	auto my_buffer = (MyBuffer *)buffer;
	auto buffer_manager = my_buffer->buffer_manager;
#ifdef DEBUG
	D_ASSERT(buffer_manager->allocated_buffers.count((data_ptr_t)buffer));
#endif

	// assert that the buffer was pinnned
	D_ASSERT(my_buffer->pinned > 0);

	my_buffer->pinned--;
	if (my_buffer->pinned == 0) {
		D_ASSERT(buffer_manager->pinned_buffers > 0);
		buffer_manager->pinned_buffers--;
	}
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

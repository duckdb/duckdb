#pragma once

#include "duckdb.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/storage/cbuffer_manager.hpp"
#include "duckdb/common/mutex.hpp"
#ifdef DEBUG
#include "duckdb/common/unordered_set.hpp"
using duckdb::data_ptr_t;
using duckdb::unordered_set;
#endif

struct MyBufferManager {
public:
	MyBufferManager() : allocated_memory(0), max_memory(12000000000) {
	}
	duckdb::idx_t allocated_memory;
	duckdb::idx_t max_memory;
#ifdef DEBUG
	duckdb::mutex lock;
	unordered_set<data_ptr_t> pinned_buffers;
	unordered_set<data_ptr_t> allocated_buffers;
	unordered_set<data_ptr_t> freed_buffers;
#endif
};

struct MyBuffer {
	MyBufferManager *buffer_manager;
	void *allocation;
	idx_t pinned;
	duckdb::idx_t size;
};

MyBuffer *CreateBuffer(void *allocation, idx_t size, MyBufferManager *buffer_manager, idx_t header_bytes);
duckdb_block Allocate(void *data, idx_t size, idx_t header_bytes);
void Destroy(void *data, duckdb_block buffer, idx_t header_bytes);
duckdb_block ReAllocate(void *data, duckdb_block buffer, idx_t old_size, idx_t new_size, idx_t header_bytes);
void *Pin(void *data, duckdb_block buffer);
void Unpin(void *data, duckdb_block buffer);
idx_t UsedMemory(void *data);
idx_t MaxMemory(void *data);

namespace duckdb {

CBufferManagerConfig DefaultCBufferManagerConfig(MyBufferManager *manager);

} // namespace duckdb

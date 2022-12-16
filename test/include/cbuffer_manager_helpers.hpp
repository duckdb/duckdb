#include "duckdb.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/storage/cbuffer_manager.hpp"
#ifdef DEBUG
#include "duckdb/common/unordered_set.hpp"
using duckdb::data_ptr_t;
using duckdb::unordered_set;
#endif

struct MyBufferManager {
	duckdb::idx_t allocated_memory;
	duckdb::idx_t pinned_buffers;
	duckdb::idx_t max_memory;
#ifdef DEBUG
	unordered_set<data_ptr_t> allocated_buffers;
#endif
};

struct MyBuffer {
	MyBufferManager *buffer_manager;
	void *allocation;
	idx_t pinned;
	duckdb::idx_t size;
};

MyBuffer *CreateBuffer(void *allocation, idx_t size, MyBufferManager *buffer_manager);
duckdb_buffer Allocate(void *data, idx_t size);
void Destroy(duckdb_buffer buffer);
duckdb_buffer ReAllocate(duckdb_buffer buffer, idx_t old_size, idx_t new_size);
void *GetAllocation(duckdb_buffer buffer);
void *Pin(duckdb_buffer buffer);
void Unpin(duckdb_buffer buffer);
idx_t UsedMemory(void *data);
idx_t MaxMemory(void *data);

namespace duckdb {

CBufferManagerConfig DefaultCBufferManagerConfig(MyBufferManager *manager);

} // namespace duckdb

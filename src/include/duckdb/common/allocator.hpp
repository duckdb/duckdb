//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
class Allocator;
class ClientContext;
class DatabaseInstance;
class ExecutionContext;
class ThreadContext;

struct AllocatorDebugInfo;

struct PrivateAllocatorData {
	PrivateAllocatorData();
	virtual ~PrivateAllocatorData();

	unique_ptr<AllocatorDebugInfo> debug_info;
};

typedef data_ptr_t (*allocate_function_ptr_t)(PrivateAllocatorData *private_data, idx_t size);
typedef void (*free_function_ptr_t)(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size);
typedef data_ptr_t (*reallocate_function_ptr_t)(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
                                                idx_t size);

class AllocatedData {
public:
	DUCKDB_API AllocatedData();
	DUCKDB_API AllocatedData(Allocator &allocator, data_ptr_t pointer, idx_t allocated_size);
	DUCKDB_API ~AllocatedData();
	// disable copy constructors
	DUCKDB_API AllocatedData(const AllocatedData &other) = delete;
	DUCKDB_API AllocatedData &operator=(const AllocatedData &) = delete;
	//! enable move constructors
	DUCKDB_API AllocatedData(AllocatedData &&other) noexcept;
	DUCKDB_API AllocatedData &operator=(AllocatedData &&) noexcept;

	data_ptr_t get() {
		return pointer;
	}
	const_data_ptr_t get() const {
		return pointer;
	}
	idx_t GetSize() const {
		return allocated_size;
	}
	void Reset();

private:
	Allocator *allocator;
	data_ptr_t pointer;
	idx_t allocated_size;
};

class Allocator {
public:
	DUCKDB_API Allocator();
	DUCKDB_API Allocator(allocate_function_ptr_t allocate_function_p, free_function_ptr_t free_function_p,
	                     reallocate_function_ptr_t reallocate_function_p,
	                     unique_ptr<PrivateAllocatorData> private_data);
	Allocator &operator=(Allocator &&allocator) noexcept = delete;
	DUCKDB_API ~Allocator();

	data_ptr_t AllocateData(idx_t size);
	void FreeData(data_ptr_t pointer, idx_t size);
	data_ptr_t ReallocateData(data_ptr_t pointer, idx_t old_size, idx_t new_size);

	AllocatedData Allocate(idx_t size) {
		return AllocatedData(*this, AllocateData(size), size);
	}

	static data_ptr_t DefaultAllocate(PrivateAllocatorData *private_data, idx_t size) {
		return (data_ptr_t)malloc(size);
	}
	static void DefaultFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
		free(pointer);
	}
	static data_ptr_t DefaultReallocate(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
	                                    idx_t size) {
		return (data_ptr_t)realloc(pointer, size);
	}
	static Allocator &Get(ClientContext &context);
	static Allocator &Get(DatabaseInstance &db);

	PrivateAllocatorData *GetPrivateData() {
		return private_data.get();
	}

	static Allocator &DefaultAllocator();

private:
	allocate_function_ptr_t allocate_function;
	free_function_ptr_t free_function;
	reallocate_function_ptr_t reallocate_function;

	unique_ptr<PrivateAllocatorData> private_data;
};

//! The BufferAllocator is a wrapper around the global allocator class that sends any allocations made through the
//! buffer manager. This makes the buffer manager aware of the memory usage, allowing it to potentially free
//! other blocks to make space in memory.
//! Note that there is a cost to doing so (several atomic operations will be performed on allocation/free).
//! As such this class should be used primarily for larger allocations.
struct BufferAllocator {
	static Allocator &Get(ClientContext &context);
};

} // namespace duckdb

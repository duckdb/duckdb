//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class Allocator;
class AttachedDatabase;
class ClientContext;
class DatabaseInstance;
class ExecutionContext;
class ThreadContext;

struct AllocatorDebugInfo;

struct PrivateAllocatorData {
	PrivateAllocatorData();
	virtual ~PrivateAllocatorData();

	unique_ptr<AllocatorDebugInfo> debug_info;

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
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
	AllocatedData(const AllocatedData &other) = delete;
	AllocatedData &operator=(const AllocatedData &) = delete;
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
	bool IsSet() {
		return pointer;
	}
	void Reset();

private:
	optional_ptr<Allocator> allocator;
	data_ptr_t pointer;
	idx_t allocated_size;
};

class Allocator {
	// 281TB ought to be enough for anybody
	static constexpr const idx_t MAXIMUM_ALLOC_SIZE = 281474976710656ULL;

public:
	DUCKDB_API Allocator();
	DUCKDB_API Allocator(allocate_function_ptr_t allocate_function_p, free_function_ptr_t free_function_p,
	                     reallocate_function_ptr_t reallocate_function_p,
	                     unique_ptr<PrivateAllocatorData> private_data);
	Allocator &operator=(Allocator &&allocator) noexcept = delete;
	DUCKDB_API ~Allocator();

	DUCKDB_API data_ptr_t AllocateData(idx_t size);
	DUCKDB_API void FreeData(data_ptr_t pointer, idx_t size);
	DUCKDB_API data_ptr_t ReallocateData(data_ptr_t pointer, idx_t old_size, idx_t new_size);

	AllocatedData Allocate(idx_t size) {
		return AllocatedData(*this, AllocateData(size), size);
	}
	static data_ptr_t DefaultAllocate(PrivateAllocatorData *private_data, idx_t size) {
		return data_ptr_cast(malloc(size));
	}
	static void DefaultFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
		free(pointer);
	}
	static data_ptr_t DefaultReallocate(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
	                                    idx_t size) {
		return data_ptr_cast(realloc(pointer, size));
	}
	static Allocator &Get(ClientContext &context);
	static Allocator &Get(DatabaseInstance &db);
	static Allocator &Get(AttachedDatabase &db);

	PrivateAllocatorData *GetPrivateData() {
		return private_data.get();
	}

	DUCKDB_API static Allocator &DefaultAllocator();
	DUCKDB_API static shared_ptr<Allocator> &DefaultAllocatorReference();

	static void ThreadFlush(idx_t threshold);

private:
	allocate_function_ptr_t allocate_function;
	free_function_ptr_t free_function;
	reallocate_function_ptr_t reallocate_function;

	unique_ptr<PrivateAllocatorData> private_data;
};

template <class T>
T *AllocateArray(idx_t size) {
	return (T *)Allocator::DefaultAllocator().AllocateData(size * sizeof(T));
}

template <class T>
void DeleteArray(T *ptr, idx_t size) {
	Allocator::DefaultAllocator().FreeData(data_ptr_cast(ptr), size * sizeof(T));
}

template <typename T, typename... ARGS>
T *AllocateObject(ARGS &&... args) {
	auto data = Allocator::DefaultAllocator().AllocateData(sizeof(T));
	return new (data) T(std::forward<ARGS>(args)...);
}

template <typename T>
void DestroyObject(T *ptr) {
	ptr->~T();
	Allocator::DefaultAllocator().FreeData(data_ptr_cast(ptr), sizeof(T));
}

//! The BufferAllocator is a wrapper around the global allocator class that sends any allocations made through the
//! buffer manager. This makes the buffer manager aware of the memory usage, allowing it to potentially free
//! other blocks to make space in memory.
//! Note that there is a cost to doing so (several atomic operations will be performed on allocation/free).
//! As such this class should be used primarily for larger allocations.
struct BufferAllocator {
	DUCKDB_API static Allocator &Get(ClientContext &context);
	DUCKDB_API static Allocator &Get(DatabaseInstance &db);
	DUCKDB_API static Allocator &Get(AttachedDatabase &db);
};

} // namespace duckdb

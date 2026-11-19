//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/capi_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
class BufferPool;

//! The allocator behind the C API's duckdb_v2_alloc family: memory owned by clients and extensions, e.g. the heap
//! behind a Rust #[global_allocator].
//!
//! It is process-wide and outlives every database, since such memory can be freed at any time. The bytes it holds are
//! charged to every live buffer pool under MemoryTag::EXTENSION, so they show up in duckdb_memory() and leave less room
//! under the memory limit. The charge is never enforced: an allocation only fails when the system is out of memory.
//!
//! Every allocation is described by its size and a power-of-two alignment, and must be freed or reallocated with the
//! same pair.
class CApiAllocator {
public:
	//! Requires size > 0. Throws an OutOfMemoryException on failure.
	static data_ptr_t Allocate(idx_t size, idx_t alignment);
	static void Free(data_ptr_t pointer, idx_t size, idx_t alignment);
	//! Requires old_size > 0 and new_size > 0. On failure the original allocation is left untouched.
	static data_ptr_t Reallocate(data_ptr_t pointer, idx_t old_size, idx_t alignment, idx_t new_size);

	//! The bytes currently held, exact, including any padding of over-aligned allocations.
	static idx_t GetAllocatedSize();

	//! Called by a buffer pool when it is created and destroyed.
	static void RegisterPool(BufferPool &pool);
	static void UnregisterPool(BufferPool &pool);
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension/capi_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"

namespace duckdb {

// Hidden, so that the C API calls it directly instead of through the PLT.
#if defined(__GNUC__) || defined(__clang__)
#define DUCKDB_EXTENSION_ALLOCATOR_HIDDEN __attribute__((visibility("hidden")))
#else
#define DUCKDB_EXTENSION_ALLOCATOR_HIDDEN
#endif

struct DUCKDB_EXTENSION_ALLOCATOR_HIDDEN ExtensionAllocator {
	static unique_ptr<Allocator> Create();
	static unique_ptr<Allocator> Create(DatabaseInstance &db);
	static void Register(Allocator &allocator, const string &extension_name);
	static optional_idx GetUsage(const string &extension_name);

	//! Blocks of an allocator made by Create(), with a power-of-two alignment. Sizes must be non-zero, and a block must
	//! be freed or reallocated with the size and alignment it was allocated with. Throw on failure.
	static data_ptr_t Allocate(Allocator &allocator, idx_t size, idx_t alignment, bool zeroed);
	static void Free(Allocator &allocator, data_ptr_t pointer, idx_t size, idx_t alignment);
	static data_ptr_t Reallocate(Allocator &allocator, data_ptr_t pointer, idx_t old_size, idx_t alignment,
	                             idx_t new_size);

	//! The same operations without argument checks: they return NULL on failure instead of throwing.
	struct RawFunctions {
		void *state;
		void *(*allocate)(void *state, idx_t size, idx_t alignment);
		void *(*allocate_zeroed)(void *state, idx_t size, idx_t alignment);
		void (*free)(void *state, void *pointer, idx_t size, idx_t alignment);
		void *(*reallocate)(void *state, void *pointer, idx_t old_size, idx_t alignment, idx_t new_size);
	};
	//! Valid for as long as `allocator` is.
	static RawFunctions GetRawFunctions(Allocator &allocator);
};

} // namespace duckdb

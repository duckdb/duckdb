//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension/capi_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {
class BufferPool;
struct CApiAllocatorData;

//! The allocator a C API extension's own memory comes from, e.g. the heap behind a Rust #[global_allocator].
//!
//! There is one per extension for the lifetime of the process: an extension library is shared by every database that
//! loads it, and its memory can be freed at any time, also after those databases have closed. The allocator counts the
//! bytes it holds and charges them to the buffer pool of every database the extension is loaded in, under
//! MemoryTag::EXTENSION, so they show up in duckdb_memory() and leave less room under the memory limit. The charge is
//! never enforced: an allocation only fails when the system itself is out of memory.
//!
//! Every allocation is described by its size and a power-of-two alignment, and must be freed or reallocated with the
//! same pair. Alignments beyond the fundamental one are served by over-allocating and storing the original pointer in
//! front of the aligned one.
class CApiAllocator : public Allocator {
public:
	//! Returns the allocator of the extension, creating it on first use. Allocators are never destroyed.
	DUCKDB_API static CApiAllocator &Get(const string &extension_name);

	const string &GetExtensionName() const {
		return extension_name;
	}
	//! The bytes currently held, including the padding of over-aligned allocations.
	DUCKDB_API idx_t GetAllocatedSize() const;

	//! Requires size > 0. Throws an OutOfMemoryException on failure.
	DUCKDB_API data_ptr_t AllocateAligned(idx_t size, idx_t alignment);
	DUCKDB_API void FreeAligned(data_ptr_t pointer, idx_t size, idx_t alignment);
	//! Requires old_size > 0 and new_size > 0. On failure the original allocation is left untouched.
	DUCKDB_API data_ptr_t ReallocateAligned(data_ptr_t pointer, idx_t old_size, idx_t alignment, idx_t new_size);

	//! Charges the held bytes to the pool until the matching Detach. A pool attached more than once is charged once.
	void Attach(shared_ptr<BufferPool> pool);
	void Detach(BufferPool &pool);

private:
	explicit CApiAllocator(string extension_name);

	CApiAllocatorData &data;
	string extension_name;
};

//! Charges a CApiAllocator to a database's buffer pool for as long as it lives.
class CApiAllocatorAttachment {
public:
	CApiAllocatorAttachment(CApiAllocator &allocator, shared_ptr<BufferPool> pool);
	~CApiAllocatorAttachment();
	CApiAllocatorAttachment(const CApiAllocatorAttachment &) = delete;
	CApiAllocatorAttachment &operator=(const CApiAllocatorAttachment &) = delete;

	CApiAllocator &GetAllocator() const {
		return allocator;
	}

private:
	CApiAllocator &allocator;
	shared_ptr<BufferPool> pool;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/block_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

class Allocator;
class AttachedDatabase;
class DatabaseInstance;
class BlockAllocatorThreadLocalState;
struct BlockQueue;

class BlockAllocator {
	friend class BlockAllocatorThreadLocalState;

public:
	BlockAllocator(Allocator &allocator, idx_t block_size, idx_t virtual_memory_size, idx_t physical_memory_size);
	~BlockAllocator();

public:
	static BlockAllocator &Get(DatabaseInstance &db);
	static BlockAllocator &Get(AttachedDatabase &db);

	//! Resize physical memory (can only be increased)
	void Resize(idx_t new_physical_memory_size);

	//! Allocation functions (same API as Allocator)
	data_ptr_t AllocateData(idx_t size) const;
	void FreeData(data_ptr_t pointer, idx_t size) const;
	data_ptr_t ReallocateData(data_ptr_t pointer, idx_t old_size, idx_t new_size) const;

	//! Flush outstanding allocations
	bool SupportsFlush() const;
	void ThreadFlush(bool allocator_background_threads, idx_t threshold, idx_t thread_count) const;
	void FlushAll(optional_idx extra_memory = optional_idx()) const;

private:
	bool IsActive() const;
	bool IsEnabled() const;
	bool IsInPool(data_ptr_t pointer) const;

	idx_t ModuloBlockSize(idx_t n) const;
	idx_t DivBlockSize(idx_t n) const;

	uint32_t GetBlockID(data_ptr_t pointer) const;
	data_ptr_t GetPointer(uint32_t block_id) const;

	void VerifyBlockID(uint32_t block_id) const;

	void FreeInternal(idx_t extra_memory) const;
	void FreeContiguousBlocks(uint32_t block_id_start, uint32_t block_id_end_including) const;

private:
	//! Identifier
	const hugeint_t uuid;
	//! Fallback allocator
	Allocator &allocator;

	//! Block size (power of two)
	const idx_t block_size;
	//! Shift for dividing by block size
	const idx_t block_size_div_shift;

	//! Size of the virtual memory
	const idx_t virtual_memory_size;
	//! Pointer to the start of the virtual memory
	const data_ptr_t virtual_memory_space;

	//! Mutex for modifying physical memory size
	mutex physical_memory_lock;
	//! Size of the physical memory
	atomic<idx_t> physical_memory_size;

	//! Untouched block IDs
	unsafe_unique_ptr<BlockQueue> untouched;
	//! Touched by block IDs
	unsafe_unique_ptr<BlockQueue> touched;
};

} // namespace duckdb

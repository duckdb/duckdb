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

namespace duckdb {

class Allocator;
class AttachedDatabase;
class DatabaseInstance;
class BlockAllocatorThreadLocalState;
struct BlockQueue;

class BlockAllocator {
	friend class BlockAllocatorThreadLocalState;

public:
	BlockAllocator(Allocator &allocator, bool enable, idx_t block_size, idx_t virtual_memory_size);
	~BlockAllocator();

public:
	static BlockAllocator &Get(DatabaseInstance &db);
	static BlockAllocator &Get(AttachedDatabase &db);

	void SetEnabled(bool enable);

	//! Allocation functions (same API as Allocator)
	data_ptr_t AllocateData(idx_t size) const;
	void FreeData(data_ptr_t pointer, idx_t size) const;
	data_ptr_t ReallocateData(data_ptr_t pointer, idx_t old_size, idx_t new_size) const;

	//! Flush outstanding allocations
	bool SupportsFlush() const;
	void ThreadFlush(bool allocator_background_threads, idx_t threshold, idx_t thread_count) const;
	void FlushAll() const;

private:
	void Resize() const;

	bool IsActive() const;
	bool IsInPool(data_ptr_t pointer) const;

	idx_t ModuloBlockSize(idx_t n) const;
	idx_t DivBlockSize(idx_t n) const;

	uint32_t GetBlockID(data_ptr_t pointer) const;
	data_ptr_t GetPointer(uint32_t block_id) const;

	void TryFreeInternal() const;
	void FreeInternal() const;
	void FreeContiguousBlocks(uint32_t block_id_start, uint32_t block_id_end_including) const;

	void VerifyBlockID(uint32_t block_id) const;

private:
	//! Identifier
	const hugeint_t uuid;
	//! Fallback allocator
	Allocator &allocator;
	//! Whether this is open for new allocations
	atomic<bool> enabled;

	//! Block size (power of two)
	const idx_t block_size;
	//! Shift for dividing by block size
	const idx_t block_size_div_shift;

	//! Size of the virtual memory
	const idx_t virtual_memory_size;
	//! Pointer to the start of the virtual memory
	const data_ptr_t virtual_memory_space;

	//! Untouched block IDs
	unsafe_unique_ptr<BlockQueue> untouched;
	//! Touched by block IDs
	unsafe_unique_ptr<BlockQueue> touched;

	//! Blocks that should be freed
	unsafe_unique_ptr<BlockQueue> to_free;
	//! Actually free freed blocks once queue size hits this threshold
	static constexpr idx_t TO_FREE_SIZE_THRESHOLD = 4096;
	//! Lock so that only one thread at a time frees
	mutable mutex to_free_lock;
};

} // namespace duckdb

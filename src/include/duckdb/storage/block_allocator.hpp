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
struct BlockQueue;

class BlockAllocator {
public:
	BlockAllocator(Allocator &allocator, idx_t block_size, idx_t virtual_memory_size, idx_t physical_memory_size);
	~BlockAllocator();

public:
	static BlockAllocator &Get(DatabaseInstance &db);
	static BlockAllocator &Get(AttachedDatabase &db);

	//! Resizes the physical memory to the given size (must be greater than or equal to the current
	void Resize(idx_t new_physical_memory_size);

	//! Allocation functions (same API as Allocator)
	data_ptr_t AllocateData(idx_t size);
	void FreeData(data_ptr_t pointer, idx_t size);
	data_ptr_t ReallocateData(data_ptr_t pointer, idx_t old_size, idx_t new_size);

private:
	bool IsActive() const;
	bool IsInPool(data_ptr_t pointer) const;

	idx_t ModuloBlockSize(idx_t n) const;
	idx_t DivBlockSize(idx_t n) const;

	uint32_t GetBlockID(data_ptr_t pointer) const;
	data_ptr_t GetPointer(uint32_t block_id) const;

	void FreeInternal();
	void FreeContiguousBlocks(uint32_t block_id_start, uint32_t block_id_end_including);

	void VerifyBlockID(uint32_t block_id) const;

private:
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

	//! Lock for changing the physical memory size
	mutex physical_memory_size_lock;
	//! Size of the physical memory
	atomic<idx_t> physical_memory_size;

	//! Untouched block IDs
	unsafe_unique_ptr<BlockQueue> untouched;
	//! Touched by block IDs
	unsafe_unique_ptr<BlockQueue> touched;

	//! Blocks that should be freed
	unsafe_unique_ptr<BlockQueue> to_free;
	//! Actually free freed blocks once queue size hits this threshold
	static constexpr idx_t TO_FREE_SIZE_THRESHOLD = 128;
	//! Free up to this many blocks in one go
	static constexpr idx_t MAXIMUM_FREE_COUNT = 32768;
	//! Lock so that only one thread at a time frees
	mutex to_free_lock;
};

} // namespace duckdb

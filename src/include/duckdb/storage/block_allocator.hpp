//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/block_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class Allocator;
class AttachedDatabase;
class DatabaseInstance;
class TaskScheduler;
struct ProducerToken;
struct BlockQueue;
struct BlockAllocatorLifetimeState;

class BlockAllocator {
	friend class BlockAllocatorThreadLocalState;
	friend class BlockAllocatorFlushTask;

public:
	BlockAllocator(Allocator &allocator, idx_t block_size, idx_t virtual_memory_size, idx_t physical_memory_size);
	~BlockAllocator();

public:
	static BlockAllocator &Get(DatabaseInstance &db);
	static BlockAllocator &Get(AttachedDatabase &db);

	//! Resize physical memory (can only be increased)
	void Resize(idx_t new_physical_memory_size) DUCKDB_EXCLUDES(physical_memory_lock);

	//! Allocation functions (same API as Allocator)
	data_ptr_t AllocateData(idx_t size) const;
	void FreeData(data_ptr_t pointer, idx_t size) const;
	data_ptr_t ReallocateData(data_ptr_t pointer, idx_t old_size, idx_t new_size) const;

	//! Flush outstanding allocations
	bool SupportsFlush() const;
	optional_idx DecayDelay() const;
	void ThreadFlush(bool allocator_background_threads, idx_t threshold, idx_t thread_count) const;
	//! Pass the owning database's scheduler to defer pool reclamation.
	void ThreadIdle(optional_ptr<TaskScheduler> scheduler = nullptr) const DUCKDB_EXCLUDES(flush_lock);
	//! Best-effort reclamation of free pool blocks and fallback allocations.
	void FlushAll(optional_idx extra_memory = optional_idx()) const noexcept DUCKDB_EXCLUDES(flush_lock);
	//! Memory freed since the last flush, which the allocator may still hold
	idx_t GetDeallocatedSinceFlush() const;
	//! Flush if at least threshold bytes were freed since the last flush, returns whether a flush happened
	bool TryFlushDeallocated(idx_t threshold) const;

private:
	enum class FlushState : uint8_t { IDLE, SCHEDULED, RESCHEDULE_REQUESTED };

	bool IsActive() const;
	bool IsEnabled() const;
	bool IsInPool(data_ptr_t pointer) const;

	idx_t ModuloBlockSize(idx_t n) const;
	idx_t DivBlockSize(idx_t n) const;

	uint32_t GetBlockID(data_ptr_t pointer) const;
	data_ptr_t GetPointer(uint32_t block_id) const;

	void VerifyBlockID(uint32_t block_id) const;

	bool TryScheduleFlush(TaskScheduler &scheduler) const DUCKDB_EXCLUDES(flush_lock);
	//! Return the unprocessed portion of the initial free-block budget.
	idx_t FlushPool(optional_idx block_limit = optional_idx(), optional_idx task_limit = optional_idx()) const noexcept
	    DUCKDB_EXCLUDES(flush_lock);
	idx_t FreeInternal(optional_idx block_limit, optional_idx task_limit) const;
	void FreeContiguousBlocks(uint32_t block_id_start, uint32_t block_id_end_including) const;

private:
	//! Identifier
	const hugeint_t uuid;
	//! Fallback allocator
	Allocator &allocator;
	//! Memory freed since the last flush
	mutable atomic<idx_t> deallocated_since_flush {0};

	//! Block size (power of two)
	const idx_t block_size;
	//! Shift for dividing by block size
	const idx_t block_size_div_shift;

	//! Size of the virtual memory
	const idx_t virtual_memory_size;
	//! Pointer to the start of the virtual memory
	atomic<data_ptr_t> virtual_memory_space;

	//! Mutex for modifying physical memory size
	annotated_mutex physical_memory_lock;
	//! Size of the physical memory
	atomic<idx_t> physical_memory_size;

	//! Untouched block IDs
	unsafe_unique_ptr<BlockQueue> untouched;
	//! Touched by block IDs
	unsafe_unique_ptr<BlockQueue> touched;

	//! Synchronizes returning cached blocks with allocator destruction.
	shared_ptr<BlockAllocatorLifetimeState> lifetime_state;

	//! Coalesce requests into one task, including follow-up passes while it runs.
	mutable annotated_mutex flush_lock;
	//! Scheduler queue destruction invalidates this token before allocator destruction.
	mutable unique_ptr<ProducerToken> flush_producer DUCKDB_GUARDED_BY(flush_lock);
	mutable FlushState flush_state DUCKDB_GUARDED_BY(flush_lock) = FlushState::IDLE;
};

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/block_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/array.hpp"
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
	friend class BlockAllocatorTestHelper;
	friend class TaskScheduler;

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

	//! Free backing retained in global queues, thread-local caches, and ongoing reclamation.
	idx_t GetCachedMemory() const;

	//! Flush outstanding allocations
	bool SupportsFlush() const;
	optional_idx DecayDelay() const;
	void ThreadFlush(bool allocator_background_threads, idx_t threshold, idx_t thread_count) const;
	//! Apply idle decay through the bound scheduler; standalone allocators reclaim synchronously.
	void ThreadIdle() const DUCKDB_EXCLUDES(flush_lock);
	//! Return local blocks without scheduling or discarding pool backing.
	void ThreadExit() const;
	//! Best-effort reclamation of all cached pool backing and fallback allocations.
	void FlushAll() const noexcept DUCKDB_EXCLUDES(flush_lock);
	//! Leave pool backing for unmapping and flush fallback allocations.
	void FlushOnShutdown() const noexcept;
	//! Retain cached backing that fits alongside the buffer manager's live reservations.
	void FlushForAllocation(idx_t extra_memory, idx_t memory_headroom) const noexcept DUCKDB_EXCLUDES(flush_lock);
	//! Purge accumulated fallback frees when they no longer fit alongside cached pool backing.
	bool TryFlushDeallocated(idx_t threshold, idx_t memory_headroom) const noexcept;

private:
	enum class FlushState : uint8_t { IDLE, SCHEDULED, RESCHEDULE_REQUESTED };
	enum class ReclaimMode : uint8_t { FORCE, DECAY, OPPORTUNISTIC };

	bool IsActive() const;
	bool IsEnabled() const;
	bool IsInPool(data_ptr_t pointer) const;

	idx_t ModuloBlockSize(idx_t n) const;
	idx_t DivBlockSize(idx_t n) const;

	uint32_t GetBlockID(data_ptr_t pointer) const;
	data_ptr_t GetPointer(uint32_t block_id) const;

	void VerifyBlockID(uint32_t block_id) const;

	void InitializeRetention(idx_t now_ms) const;
	void AddRetention(idx_t count, idx_t now_ms) const;
	void ReuseRetention(idx_t count, idx_t now_ms) const;
	idx_t RetentionTarget(idx_t now_ms) const;
	void UpdateCachedBlocks(int64_t count, idx_t slot) const;
	void ReturnThreadLocalBlocks() const;
	void FlushFallbackAllocator(idx_t deallocated) const noexcept;

	void SetScheduler(TaskScheduler &scheduler);
	void ClearScheduler();

	bool TryScheduleFlush(TaskScheduler &scheduler) const DUCKDB_EXCLUDES(flush_lock);
	idx_t GetFreeBlockCount() const DUCKDB_REQUIRES(flush_lock);
	idx_t GetReclaimableBlockCount(ReclaimMode mode, optional_idx cache_limit = optional_idx()) const
	    DUCKDB_REQUIRES(flush_lock);
	//! Return the unprocessed portion of the initial free-block budget.
	idx_t FlushPool(optional_idx block_limit = optional_idx(), optional_idx task_limit = optional_idx(),
	                ReclaimMode mode = ReclaimMode::FORCE, optional_idx cache_limit = optional_idx()) const noexcept
	    DUCKDB_EXCLUDES(flush_lock);
	idx_t FreeInternal(optional_idx block_limit, optional_idx task_limit, ReclaimMode mode,
	                   optional_idx cache_limit = optional_idx()) const DUCKDB_EXCLUDES(flush_lock);
	bool FreeContiguousBlocks(uint32_t block_id_start, uint32_t block_id_end_including) const;

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
	atomic<data_ptr_t> virtual_memory_space;

	//! Mutex for modifying physical memory size
	annotated_mutex physical_memory_lock;
	//! Size of the physical memory
	atomic<idx_t> physical_memory_size;

	//! Untouched block IDs
	unsafe_unique_ptr<BlockQueue> untouched;
	//! Touched by block IDs
	unsafe_unique_ptr<BlockQueue> touched;

	struct alignas(64) CachedBlockCount {
		//! Cross-thread reuse can make individual balances negative.
		atomic<int64_t> count {0};
	};
	//! Separate writers so each block reuse does not contend on a single counter.
	mutable array<CachedBlockCount, 64> cached_blocks;
	mutable atomic<idx_t> next_cache_slot {0};
	//! Still charged to cached_blocks until discard succeeds.
	mutable idx_t reclaiming_blocks DUCKDB_GUARDED_BY(flush_lock) = 0;

	//! Synchronizes returning cached blocks with allocator destruction.
	shared_ptr<BlockAllocatorLifetimeState> lifetime_state;

	//! Approximate retention volume, with a generation and count packed into each bucket.
	mutable array<atomic<uint64_t>, 3> retention_buckets;
	//! Protect maintenance decisions and background scheduling.
	mutable annotated_mutex flush_lock;
	//! Scheduler queue destruction invalidates this token before allocator destruction.
	mutable unique_ptr<ProducerToken> flush_producer DUCKDB_GUARDED_BY(flush_lock);
	mutable FlushState flush_state DUCKDB_GUARDED_BY(flush_lock) = FlushState::IDLE;
	//! Bound before workers start and cleared after they join; never owns the scheduler.
	optional_ptr<TaskScheduler> scheduler;

	//! Keep fallback free traffic separate from fields used by pooled allocations.
	alignas(64) mutable atomic<idx_t> deallocated_since_flush {0};
};

} // namespace duckdb

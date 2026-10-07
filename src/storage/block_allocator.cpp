#include "duckdb/storage/block_allocator.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/concurrentqueue.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/chrono.hpp"

#if defined(_WIN32)
#include "duckdb/common/windows.hpp"
#else
#include <sys/mman.h>
#endif
#ifdef __MVS__
#include <zos-tls.h>
#endif

namespace duckdb {

struct BlockAllocatorConfig {
	//! Blocks transferred between thread-local caches and global queues.
	static constexpr idx_t BATCH_SIZE = 16;
	//! Cached freed blocks that trigger a transfer to the global queue.
	static constexpr idx_t FREE_THRESHOLD = BATCH_SIZE * 2;
	//! Idle decay delay in seconds when the fallback allocator supplies none.
	static constexpr idx_t DEFAULT_DECAY_DELAY = 1;
	//! Maximum blocks sorted and coalesced in one reclamation batch.
	static constexpr idx_t RECLAIM_BATCH_SIZE = 1024;
	//! Maximum blocks reclaimed by one background task.
	static constexpr idx_t MAX_FLUSH_BLOCKS = 64;
	//! Width of each retention bucket, rounding return times up to the bucket end.
	static constexpr idx_t RETENTION_INTERVAL_MS = 500;
	//! Linear decay on the same one-second timescale as bundled jemalloc, without extra grace.
	static constexpr idx_t RETENTION_DECAY_MS = DEFAULT_DECAY_DELAY * 1000;
	//! Current bucket plus the complete decay history.
	static constexpr idx_t RETENTION_BUCKETS = RETENTION_DECAY_MS / RETENTION_INTERVAL_MS + 1;
};

static idx_t RetentionTimeMillis() {
	return NumericCast<idx_t>(duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count());
}

void BlockAllocator::AdvanceRetention(const idx_t now_ms) const {
	const auto next_epoch = now_ms / BlockAllocatorConfig::RETENTION_INTERVAL_MS;
	D_ASSERT(next_epoch >= retention_epoch);
	if (next_epoch - retention_epoch >= retention_buckets.size()) {
		std::fill(retention_buckets.begin(), retention_buckets.end(), 0);
	} else {
		for (auto i = retention_epoch; i < next_epoch; i++) {
			retention_buckets[(i + 1) % retention_buckets.size()] = 0;
		}
	}
	retention_epoch = next_epoch;
}

void BlockAllocator::AddRetention(const idx_t count, const idx_t now_ms) const {
	AdvanceRetention(now_ms);
	const auto capacity = DivBlockSize(physical_memory_size.load());
	auto &bucket = retention_buckets[retention_epoch % retention_buckets.size()];
	bucket += MinValue(count, capacity - bucket);
}

void BlockAllocator::ReuseRetention(idx_t count) const {
	for (idx_t age = 0; age < retention_buckets.size() && count > 0; age++) {
		auto &bucket = retention_buckets[(retention_epoch % retention_buckets.size() + retention_buckets.size() - age) %
		                                 retention_buckets.size()];
		const auto reused = MinValue(count, bucket);
		bucket -= reused;
		count -= reused;
	}
}

idx_t BlockAllocator::RetentionTarget(const idx_t now_ms) const {
	AdvanceRetention(now_ms);
	idx_t weighted = 0;
	const auto offset = now_ms % BlockAllocatorConfig::RETENTION_INTERVAL_MS;
	for (idx_t i = 0; i < retention_buckets.size(); i++) {
		const auto count =
		    retention_buckets[(retention_epoch % retention_buckets.size() + retention_buckets.size() - i) %
		                      retention_buckets.size()];
		const auto age_ms = i == 0 ? 0 : (i - 1) * BlockAllocatorConfig::RETENTION_INTERVAL_MS + offset;
		const auto remaining_ms = BlockAllocatorConfig::RETENTION_DECAY_MS - age_ms;
		weighted += count * remaining_ms;
	}
	return weighted / BlockAllocatorConfig::RETENTION_DECAY_MS;
}

//===--------------------------------------------------------------------===//
// Memory Helpers
//===--------------------------------------------------------------------===//
static data_ptr_t AllocateVirtualMemory(const idx_t size) {
#if INTPTR_MAX == INT32_MAX
	// Disable on 32-bit
	return nullptr;
#endif

#if defined(_WIN32)
	// This returns nullptr on failure
	return data_ptr_t(VirtualAlloc(nullptr, size, MEM_RESERVE, PAGE_NOACCESS));
#else
	const auto ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
	return ptr == MAP_FAILED ? nullptr : data_ptr_cast(ptr);
#endif
}

static void FreeVirtualMemory(const data_ptr_t pointer, const idx_t size) {
	bool success;
#if defined(_WIN32)
	success = VirtualFree(pointer, 0, MEM_RELEASE);
#else
	success = munmap(pointer, size) == 0;
#endif
	if (!success) {
		throw InternalException("FreeVirtualMemory failed");
	}
}

static void OnFirstAllocation(const data_ptr_t pointer, const idx_t size) {
	bool success = true;
#if defined(_WIN32)
	success = VirtualAlloc(pointer, size, MEM_COMMIT, PAGE_READWRITE);
#endif
	if (!success) {
		throw InternalException("OnFirstAllocation failed");
	}
}

static void OnDeallocation(const data_ptr_t pointer, const idx_t size) {
	bool success;
#if defined(_WIN32)
	success = VirtualFree(pointer, size, MEM_DECOMMIT);
#elif defined(__APPLE__)
	success = madvise(pointer, size, MADV_FREE_REUSABLE) == 0;
#elif defined(__MVS__)
	// the madvice functionality is not available on z/OS in any form
	success = true;
#else
	success = madvise(pointer, size, MADV_DONTNEED) == 0;
#endif
	if (!success) {
		throw InternalException("OnDeallocation failed");
	}
}

//===--------------------------------------------------------------------===//
// BlockAllocatorThreadLocalState
//===--------------------------------------------------------------------===//
struct BlockQueue {
	duckdb_moodycamel::ConcurrentQueue<uint32_t> q;
};

struct BlockAllocatorLifetimeState {
	annotated_mutex lock;
	bool alive DUCKDB_GUARDED_BY(lock) = true;
};

class BlockAllocatorThreadLocalState {
public:
	explicit BlockAllocatorThreadLocalState(const BlockAllocator &block_allocator_p) {
		Initialize(block_allocator_p);
	}
	~BlockAllocatorThreadLocalState() {
		Clear();
	}

public:
	void TryInitialize(const BlockAllocator &block_allocator_p) {
		// Local state can be invalidated if DB closes but thread stays alive
		if (cached_uuid != block_allocator_p.uuid) {
			Initialize(block_allocator_p);
		}
	}

	data_ptr_t Allocate() {
		auto pointer = TryAllocateFromLocal();
		if (pointer) {
			return pointer;
		}

		// We have run out of local blocks
		if (TryGetBatch(touched, *block_allocator->touched) || TryGetBatch(untouched, *block_allocator->untouched)) {
			// We have refilled local blocks
			pointer = TryAllocateFromLocal();
			D_ASSERT(pointer);
			return pointer;
		}

		// We have also run out of global blocks, use fallback allocator
		return block_allocator->allocator.AllocateData(block_allocator->block_size);
	}

	void Free(const data_ptr_t pointer) {
		touched.push_back(block_allocator->GetBlockID(pointer));
		if (touched.size() < BlockAllocatorConfig::FREE_THRESHOLD) {
			return;
		}

		// Upon reaching the threshold, we return a local batch to global
		std::sort(touched.begin(), touched.end());
		ReturnTouched(BlockAllocatorConfig::BATCH_SIZE);
	}

	void Clear() DUCKDB_EXCLUDES(lifetime_state->lock) {
		if (lifetime_state) {
			annotated_lock_guard<annotated_mutex> guard(lifetime_state->lock);
			if (lifetime_state->alive) {
				if (!touched.empty()) {
					ReturnTouched(touched.size());
				}
				if (!untouched.empty()) {
					block_allocator->untouched->q.enqueue_bulk(untouched.begin(), untouched.size());
				}
			}
		}
		touched.clear();
		untouched.clear();
	}

private:
	void ReturnTouched(const idx_t count) DUCKDB_EXCLUDES(block_allocator->flush_lock) {
		annotated_lock_guard<annotated_mutex> guard(block_allocator->flush_lock);
		block_allocator->touched->q.enqueue_bulk(touched.end() - count, count);
		block_allocator->AddRetention(count, RetentionTimeMillis());
		touched.resize(touched.size() - count);
	}

	void Initialize(const BlockAllocator &block_allocator_p) {
		Clear();
		cached_uuid = block_allocator_p.uuid;
		block_allocator = block_allocator_p;
		lifetime_state = block_allocator_p.lifetime_state;
		untouched.reserve(BlockAllocatorConfig::BATCH_SIZE);
		touched.reserve(BlockAllocatorConfig::FREE_THRESHOLD);
	}

	data_ptr_t TryAllocateFromLocal() {
		if (!touched.empty()) {
			const auto pointer = block_allocator->GetPointer(touched.back());
			touched.pop_back();
			return pointer;
		}
		if (!untouched.empty()) {
			const auto pointer = block_allocator->GetPointer(untouched.back());
			untouched.pop_back();
			OnFirstAllocation(pointer, block_allocator->block_size);
			return pointer;
		}
		return nullptr;
	}

	bool TryGetBatch(vector<uint32_t> &local, BlockQueue &global) DUCKDB_EXCLUDES(block_allocator->flush_lock) {
		D_ASSERT(local.empty());
		local.resize(BlockAllocatorConfig::BATCH_SIZE);
		idx_t size;
		if (RefersToSameObject(global, *block_allocator->touched)) {
			annotated_lock_guard<annotated_mutex> guard(block_allocator->flush_lock);
			size = global.q.try_dequeue_bulk(local.begin(), BlockAllocatorConfig::BATCH_SIZE);
			block_allocator->ReuseRetention(size);
		} else {
			size = global.q.try_dequeue_bulk(local.begin(), BlockAllocatorConfig::BATCH_SIZE);
		}
		local.resize(size);
		std::sort(local.begin(), local.end());
		return !local.empty();
	}

private:
	hugeint_t cached_uuid;
	optional_ptr<const BlockAllocator> block_allocator;
	shared_ptr<BlockAllocatorLifetimeState> lifetime_state;

	vector<uint32_t> untouched;
	vector<uint32_t> touched;
};

BlockAllocatorThreadLocalState &GetBlockAllocatorThreadLocalState(const BlockAllocator &block_allocator) {
#ifdef __MVS__
	auto allocator_state = BlockAllocatorThreadLocalState(block_allocator);
	static __tlssim<BlockAllocatorThreadLocalState> local_state_impl(allocator_state);
	auto *local_state = local_state_impl.access();
	(*local_state).TryInitialize(block_allocator);
	return *local_state;
#else
	thread_local BlockAllocatorThreadLocalState local_state(block_allocator);
	local_state.TryInitialize(block_allocator);
	return local_state;
#endif
}

//===--------------------------------------------------------------------===//
// BlockAllocator
//===--------------------------------------------------------------------===//
BlockAllocator::BlockAllocator(Allocator &allocator_p, const idx_t block_size_p, const idx_t virtual_memory_size_p,
                               const idx_t physical_memory_size_p)
    : uuid(UUID::GenerateRandomUUID()), allocator(allocator_p), block_size(block_size_p),
      block_size_div_shift(CountZeros<idx_t>::Trailing(block_size)),
      virtual_memory_size(AlignValue(virtual_memory_size_p, block_size)), virtual_memory_space(nullptr),
      physical_memory_size(0), untouched(make_unsafe_uniq<BlockQueue>()), touched(make_unsafe_uniq<BlockQueue>()),
      lifetime_state(make_shared_ptr<BlockAllocatorLifetimeState>()),
      retention_buckets(BlockAllocatorConfig::RETENTION_BUCKETS, 0) {
	D_ASSERT(IsPowerOfTwo(block_size));
	Resize(physical_memory_size_p);
}

BlockAllocator::~BlockAllocator() {
	{
		annotated_lock_guard<annotated_mutex> guard(lifetime_state->lock);
		lifetime_state->alive = false;
	}
	GetBlockAllocatorThreadLocalState(*this).Clear();
	if (IsActive()) {
		try {
			FreeVirtualMemory(virtual_memory_space, virtual_memory_size);
		} catch (...) {
			// Not allowed to throw in destructor!
		}
	}
}

BlockAllocator &BlockAllocator::Get(DatabaseInstance &db) {
	return *db.config.block_allocator;
}

BlockAllocator &BlockAllocator::Get(AttachedDatabase &db) {
	return Get(db.GetDatabase());
}

void BlockAllocator::Resize(const idx_t new_physical_memory_size) {
	annotated_lock_guard<annotated_mutex> guard(physical_memory_lock);

	if (new_physical_memory_size != 0 && !IsActive()) {
		virtual_memory_space = AllocateVirtualMemory(virtual_memory_size);
		if (!IsActive()) {
			return; // Failed to initialize
		}
	}

	if (new_physical_memory_size < physical_memory_size) {
		throw InvalidInputException("The \"block_allocator_size\" setting cannot be reduced (current: %llu)",
		                            physical_memory_size.load());
	}
	if (new_physical_memory_size > virtual_memory_size) {
		throw InvalidInputException("The \"block_allocator_size\" setting cannot be greater than the virtual memory "
		                            "size (virtual memory size: %llu)",
		                            virtual_memory_size);
	}

	// Enqueue block IDs efficiently in batches
	uint32_t block_ids[STANDARD_VECTOR_SIZE];
	const auto start = NumericCast<uint32_t>(DivBlockSize(physical_memory_size));
	const auto end = NumericCast<uint32_t>(DivBlockSize(new_physical_memory_size));
	for (auto block_id = start; block_id < end; block_id += STANDARD_VECTOR_SIZE) {
		const auto next = MinValue<idx_t>(end - block_id, STANDARD_VECTOR_SIZE);
		for (uint32_t i = 0; i < next; i++) {
			block_ids[i] = block_id + i;
		}
		untouched->q.enqueue_bulk(block_ids, next);
	}

	// Finally, update to the new size
	physical_memory_size = new_physical_memory_size;
}

bool BlockAllocator::IsActive() const {
	return virtual_memory_space.load(std::memory_order_relaxed);
}

bool BlockAllocator::IsEnabled() const {
	return physical_memory_size.load(std::memory_order_relaxed) != 0;
}

bool BlockAllocator::IsInPool(const data_ptr_t pointer) const {
	const auto virtual_memory_space_loaded = virtual_memory_space.load(std::memory_order_relaxed);
	return pointer >= virtual_memory_space_loaded && pointer < virtual_memory_space_loaded + virtual_memory_size;
}

idx_t BlockAllocator::ModuloBlockSize(const idx_t n) const {
	return n & (block_size - 1);
}

idx_t BlockAllocator::DivBlockSize(const idx_t n) const {
	return n >> block_size_div_shift;
}

uint32_t BlockAllocator::GetBlockID(const data_ptr_t pointer) const {
	D_ASSERT(IsInPool(pointer));
	const auto offset = NumericCast<idx_t>(pointer - virtual_memory_space.load(std::memory_order_relaxed));
	D_ASSERT(ModuloBlockSize(offset) == 0);
	const auto block_id = NumericCast<uint32_t>(DivBlockSize(offset));
	VerifyBlockID(block_id);
	return block_id;
}

void BlockAllocator::VerifyBlockID(const uint32_t block_id) const {
	D_ASSERT(block_id < NumericCast<uint32_t>(virtual_memory_size / block_size));
}

data_ptr_t BlockAllocator::GetPointer(const uint32_t block_id) const {
	VerifyBlockID(block_id);
	return virtual_memory_space.load(std::memory_order_relaxed) + NumericCast<idx_t>(block_id) * block_size;
}

data_ptr_t BlockAllocator::AllocateData(const idx_t size) const {
	if (!IsActive() || !IsEnabled() || size != block_size) {
		return allocator.AllocateData(size);
	}
	return GetBlockAllocatorThreadLocalState(*this).Allocate();
}

void BlockAllocator::FreeData(const data_ptr_t pointer, const idx_t size) const {
	deallocated_since_flush.fetch_add(size, std::memory_order_relaxed);
	if (!IsActive() || !IsInPool(pointer)) {
		return allocator.FreeData(pointer, size);
	}
	D_ASSERT(size == block_size);
	GetBlockAllocatorThreadLocalState(*this).Free(pointer);
}

data_ptr_t BlockAllocator::ReallocateData(const data_ptr_t pointer, const idx_t old_size, const idx_t new_size) const {
	if (old_size == new_size) {
		return pointer;
	}

	// If both the old and new allocation are not (or cannot be) in the pool, immediately use the fallback allocator
	if (!IsActive() || (!IsInPool(pointer) && new_size != block_size)) {
		return allocator.ReallocateData(pointer, old_size, new_size);
	}

	// Either old or new can be in the pool: allocate, copy, and free
	const auto new_pointer = AllocateData(new_size);
	memcpy(new_pointer, pointer, MinValue(old_size, new_size));
	FreeData(pointer, old_size);
	return new_pointer;
}

bool BlockAllocator::SupportsFlush() const {
	return (IsActive() && IsEnabled()) || Allocator::SupportsFlush();
}

optional_idx BlockAllocator::DecayDelay() const {
	auto delay = Allocator::DecayDelay();
	if (!delay.IsValid() && IsActive() && IsEnabled()) {
		return optional_idx(BlockAllocatorConfig::DEFAULT_DECAY_DELAY);
	}
	return delay;
}

void BlockAllocator::ThreadFlush(bool allocator_background_threads, idx_t threshold, idx_t thread_count) const {
	if (IsActive() && IsEnabled()) {
		GetBlockAllocatorThreadLocalState(*this).Clear();
	}
	if (Allocator::SupportsFlush()) {
		Allocator::ThreadFlush(allocator_background_threads, threshold, thread_count);
	}
}

class BlockAllocatorFlushTask : public Task {
public:
	BlockAllocatorFlushTask(TaskScheduler &scheduler, const BlockAllocator &allocator,
	                        optional_idx remaining_blocks = optional_idx())
	    : scheduler(scheduler), allocator(allocator), remaining_blocks(remaining_blocks) {
	}

	TaskExecutionResult Execute(TaskExecutionMode mode) override {
		using FlushState = BlockAllocator::FlushState;
		{
			annotated_lock_guard<annotated_mutex> guard(allocator.flush_lock);
			D_ASSERT(allocator.flush_state == FlushState::SCHEDULED ||
			         allocator.flush_state == FlushState::RESCHEDULE_REQUESTED);
			if (!remaining_blocks.IsValid()) {
				allocator.flush_state = FlushState::SCHEDULED;
			}
		}
		const auto remaining = allocator.FlushPool(remaining_blocks, BlockAllocatorConfig::MAX_FLUSH_BLOCKS,
		                                           BlockAllocator::ReclaimMode::DECAY);
		annotated_lock_guard<annotated_mutex> guard(allocator.flush_lock);
		const bool reschedule = allocator.flush_state == FlushState::RESCHEDULE_REQUESTED;
		allocator.flush_state = FlushState::IDLE;
		// Defer to the next idle interval when other work is waiting.
		if ((remaining > 0 || reschedule) && scheduler.GetNumberOfTasks() == 0 &&
		    scheduler.NumberOfAsyncThreads() > 0 &&
		    allocator.GetReclaimableBlockCount(BlockAllocator::ReclaimMode::DECAY) > 0) {
			try {
				scheduler.ScheduleTask(
				    *allocator.flush_producer,
				    make_shared_ptr<BlockAllocatorFlushTask>(scheduler, allocator,
				                                             remaining > 0 ? optional_idx(remaining) : optional_idx()),
				    TaskSchedulerType::ASYNC);
				allocator.flush_state =
				    remaining > 0 && reschedule ? FlushState::RESCHEDULE_REQUESTED : FlushState::SCHEDULED;
			} catch (...) {
				// A later idle request can retry failed submission.
			}
		}
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	TaskScheduler &scheduler;
	const BlockAllocator &allocator;
	const optional_idx remaining_blocks;
};

bool BlockAllocator::TryScheduleFlush(TaskScheduler &scheduler) const {
	annotated_lock_guard<annotated_mutex> guard(flush_lock);
	if (scheduler.NumberOfAsyncThreads() == 0) {
		return false;
	}
	if (flush_state != FlushState::IDLE) {
		flush_state = FlushState::RESCHEDULE_REQUESTED;
		return true;
	}
	if (GetReclaimableBlockCount(ReclaimMode::DECAY) == 0) {
		return true;
	}
	try {
		if (!flush_producer) {
			flush_producer = scheduler.CreateProducer();
		}
		scheduler.ScheduleTask(*flush_producer, make_shared_ptr<BlockAllocatorFlushTask>(scheduler, *this),
		                       TaskSchedulerType::ASYNC);
		flush_state = FlushState::SCHEDULED;
		return true;
	} catch (...) {
		return false;
	}
}

void BlockAllocator::ThreadIdle(optional_ptr<TaskScheduler> scheduler) const {
	try {
		if (IsActive() && IsEnabled()) {
			GetBlockAllocatorThreadLocalState(*this).Clear();
			if (!scheduler || !TryScheduleFlush(*scheduler)) {
				FlushPool(optional_idx(), optional_idx(), scheduler ? ReclaimMode::DECAY : ReclaimMode::FORCE);
			}
		}
	} catch (...) {
		// Reclamation is best effort on scheduler threads.
	}
	Allocator::ThreadIdle();
}

idx_t BlockAllocator::GetFreeBlockCount() const {
	return touched->q.size_approx();
}

idx_t BlockAllocator::GetReclaimableBlockCount(const ReclaimMode mode) const {
	const auto available = GetFreeBlockCount();
	if (mode == ReclaimMode::FORCE) {
		return available;
	}
	return available - MinValue(available, RetentionTarget(RetentionTimeMillis()));
}

idx_t BlockAllocator::FlushPool(const optional_idx block_limit, const optional_idx task_limit,
                                const ReclaimMode mode) const noexcept {
	try {
		return FreeInternal(block_limit, task_limit, mode);
	} catch (...) {
		// Failed reclamation leaves blocks available for reuse.
		return 0;
	}
}

void BlockAllocator::FlushAll(const optional_idx extra_memory) const noexcept {
	deallocated_since_flush.store(0, std::memory_order_relaxed);
	FlushPool(extra_memory.IsValid() ? optional_idx(DivBlockSize(extra_memory.GetIndex())) : optional_idx());
	try {
		if (Allocator::SupportsFlush()) {
			Allocator::FlushAll();
		}
	} catch (...) {
		// Fallback reclamation is also best effort.
	}
}

idx_t BlockAllocator::GetDeallocatedSinceFlush() const {
	return deallocated_since_flush.load(std::memory_order_relaxed);
}

bool BlockAllocator::TryFlushDeallocated(const idx_t threshold) const {
	if (!SupportsFlush()) {
		return false;
	}
	auto deallocated = deallocated_since_flush.load(std::memory_order_relaxed);
	if (deallocated < threshold) {
		return false;
	}
	// the thread that claims the accumulated amount performs the flush
	if (!deallocated_since_flush.compare_exchange_strong(deallocated, 0, std::memory_order_relaxed)) {
		return false;
	}
	FlushAll(deallocated);
	return true;
}

idx_t BlockAllocator::FreeInternal(const optional_idx block_limit, const optional_idx task_limit,
                                   const ReclaimMode mode) const {
	if (!IsActive() || !IsEnabled()) {
		return 0;
	}
	GetBlockAllocatorThreadLocalState(*this).Clear();
	// Bound this flush even when other threads keep freeing blocks.
	idx_t remaining;
	{
		annotated_lock_guard<annotated_mutex> guard(flush_lock);
		remaining = GetReclaimableBlockCount(mode);
	}
	if (block_limit.IsValid()) {
		remaining = MinValue(remaining, block_limit.GetIndex());
	}
	auto task_remaining = task_limit.IsValid() ? MinValue(remaining, task_limit.GetIndex()) : remaining;
	const auto batch_size =
	    mode == ReclaimMode::DECAY ? BlockAllocatorConfig::MAX_FLUSH_BLOCKS : BlockAllocatorConfig::RECLAIM_BATCH_SIZE;
	unsafe_vector<uint32_t> to_free_buffer;
	to_free_buffer.resize(MinValue(task_remaining, batch_size));
	while (task_remaining > 0) {
		idx_t count;
		{
			annotated_lock_guard<annotated_mutex> guard(flush_lock);
			const auto eligible = MinValue(task_remaining, GetReclaimableBlockCount(mode));
			count = touched->q.try_dequeue_bulk(to_free_buffer.begin(), MinValue(eligible, batch_size));
		}
		if (count == 0) {
			return 0;
		}
		remaining -= count;
		task_remaining -= count;
		std::sort(to_free_buffer.begin(), to_free_buffer.begin() + count);

		idx_t start = 0;
		while (start < count) {
			auto end = start + 1;
			while (end < count && to_free_buffer[end] == to_free_buffer[end - 1] + 1) {
				end++;
			}
			try {
				FreeContiguousBlocks(to_free_buffer[start], to_free_buffer[end - 1]);
			} catch (...) {
				annotated_lock_guard<annotated_mutex> guard(flush_lock);
				touched->q.enqueue_bulk(to_free_buffer.begin() + start, count - start);
				throw;
			}
			untouched->q.enqueue_bulk(to_free_buffer.begin() + start, end - start);
			start = end;
		}
	}
	return remaining;
}

void BlockAllocator::FreeContiguousBlocks(const uint32_t block_id_start, const uint32_t block_id_end_including) const {
	const auto pointer = GetPointer(block_id_start);
	const auto num_blocks = block_id_end_including - block_id_start + 1;
	const auto size = num_blocks * block_size;
	OnDeallocation(pointer, size);
}

} // namespace duckdb

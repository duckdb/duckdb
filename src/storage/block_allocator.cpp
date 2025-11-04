#include "duckdb/storage/block_allocator.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/concurrentqueue.hpp"
#include "duckdb/common/types/uuid.hpp"

#if defined(_WIN32)
#include "duckdb/common/windows.hpp"
#else
#include <sys/mman.h>
#endif

namespace duckdb {

//===--------------------------------------------------------------------===//
// Memory Helpers
//===--------------------------------------------------------------------===//
static data_ptr_t AllocateVirtualMemory(const idx_t size) {
#if INTPTR_MAX == INT32_MAX
	// Disable on 32-bit
	return nullptr;
#endif

#if defined(_WIN32)
	// Disable on Windows until we do more testing there
	return nullptr;
	// This returns nullptr on failure
	// return data_ptr_t(VirtualAlloc(nullptr, size, MEM_RESERVE, PAGE_NOACCESS));
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

static void OnDeallocation(const data_ptr_t pointer, const idx_t size) {
	bool success;
#if defined(_WIN32)
	success = VirtualFree(pointer, size, MEM_RESET);
#elif defined(__APPLE__)
	success = madvise(pointer, size, MADV_FREE_REUSABLE) == 0;
#else
	success = madvise(pointer, size, MADV_FREE) == 0;
#endif
	if (!success) {
		throw InternalException("OnDeallocation failed");
	}
}

static void OnFirstAllocation(const data_ptr_t pointer, const idx_t size) {
	bool success = true;
#if defined(_WIN32)
	success = VirtualAlloc(pointer, size, MEM_COMMIT, PAGE_READWRITE);
#elif defined(__APPLE__)
	// Nothing to do here
#else
	for (idx_t i = 0; i < size; i += 4096) {
		pointer[i] = 0;
	}
#endif
	if (!success) {
		throw InternalException("OnFirstAllocation failed");
	}
}

//===--------------------------------------------------------------------===//
// BlockAllocatorThreadLocalState
//===--------------------------------------------------------------------===//
struct BlockQueue {
	duckdb_moodycamel::ConcurrentQueue<uint32_t> q;
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
		if (TryGetBatch(touched, *block_allocator->to_free) || TryGetBatch(touched, *block_allocator->touched) ||
		    TryGetBatch(untouched, *block_allocator->untouched)) {
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
		if (touched.size() < FREE_THRESHOLD) {
			return;
		}

		// Upon reaching the threshold, we return a local batch to global
		block_allocator->to_free->q.enqueue_bulk(touched.end() - BATCH_SIZE, BATCH_SIZE);
		block_allocator->TryFreeInternal();
		touched.resize(touched.size() - BATCH_SIZE);
	}

	void Clear() {
		// Return all local blocks back to global
		if (!touched.empty()) {
			block_allocator->to_free->q.enqueue_bulk(touched.begin(), touched.size());
			block_allocator->TryFreeInternal();
			touched.clear();
		}
		if (!untouched.empty()) {
			block_allocator->untouched->q.enqueue_bulk(untouched.begin(), untouched.size());
			untouched.clear();
		}
	}

private:
	void Initialize(const BlockAllocator &block_allocator_p) {
		cached_uuid = block_allocator_p.uuid;
		block_allocator = block_allocator_p;
		untouched.clear();
		touched.clear();
		untouched.reserve(BATCH_SIZE);
		touched.reserve(FREE_THRESHOLD);
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

	static bool TryGetBatch(vector<uint32_t> &local, BlockQueue &global) {
		D_ASSERT(local.empty());
		local.resize(BATCH_SIZE);
		const auto size = global.q.try_dequeue_bulk(local.begin(), BATCH_SIZE);
		local.resize(size);
		return !local.empty();
	}

private:
	hugeint_t cached_uuid;
	optional_ptr<const BlockAllocator> block_allocator;

	static constexpr idx_t BATCH_SIZE = 128;
	static constexpr idx_t FREE_THRESHOLD = BATCH_SIZE * 2;

	vector<uint32_t> untouched;
	vector<uint32_t> touched;
};

BlockAllocatorThreadLocalState &GetBlockAllocatorThreadLocalState(const BlockAllocator &block_allocator) {
	thread_local BlockAllocatorThreadLocalState local_state(block_allocator);
	local_state.TryInitialize(block_allocator);
	return local_state;
}

//===--------------------------------------------------------------------===//
// BlockAllocator
//===--------------------------------------------------------------------===//
BlockAllocator::BlockAllocator(Allocator &allocator_p, bool enable, const idx_t block_size_p,
                               const idx_t virtual_memory_size_p)
    : uuid(UUID::GenerateRandomUUID()), allocator(allocator_p), enabled(enable), block_size(block_size_p),
      block_size_div_shift(CountZeros<idx_t>::Trailing(block_size)),
      virtual_memory_size(AlignValue(virtual_memory_size_p, block_size)),
      virtual_memory_space(AllocateVirtualMemory(virtual_memory_size)), untouched(make_unsafe_uniq<BlockQueue>()),
      touched(make_unsafe_uniq<BlockQueue>()), to_free(make_unsafe_uniq<BlockQueue>()) {
	D_ASSERT(IsPowerOfTwo(block_size));
	Resize();
}

BlockAllocator::~BlockAllocator() {
	GetBlockAllocatorThreadLocalState(*this).Clear();
	if (IsActive()) {
		FreeVirtualMemory(virtual_memory_space, virtual_memory_size);
	}
}

BlockAllocator &BlockAllocator::Get(DatabaseInstance &db) {
	return *db.config.block_allocator;
}

BlockAllocator &BlockAllocator::Get(AttachedDatabase &db) {
	return Get(db.GetDatabase());
}

void BlockAllocator::SetEnabled(bool enable) {
	enabled = enable;
}

void BlockAllocator::Resize() const {
	if (!IsActive()) {
		return;
	}

	// Enqueue block IDs efficiently in batches
	uint32_t block_ids[STANDARD_VECTOR_SIZE];
	const auto end = NumericCast<uint32_t>(virtual_memory_size / block_size);
	for (uint32_t block_id = 0; block_id < end; block_id += STANDARD_VECTOR_SIZE) {
		const auto next = MinValue<idx_t>(end - block_id, STANDARD_VECTOR_SIZE);
		for (uint32_t i = 0; i < next; i++) {
			block_ids[i] = block_id + i;
		}
		untouched->q.enqueue_bulk(block_ids, next);
	}
}

bool BlockAllocator::IsActive() const {
	return virtual_memory_space;
}

bool BlockAllocator::IsInPool(const data_ptr_t pointer) const {
	return pointer >= virtual_memory_space && pointer < virtual_memory_space + virtual_memory_size;
}

idx_t BlockAllocator::ModuloBlockSize(const idx_t n) const {
	return n & (block_size - 1);
}

idx_t BlockAllocator::DivBlockSize(const idx_t n) const {
	return n >> block_size_div_shift;
}

uint32_t BlockAllocator::GetBlockID(const data_ptr_t pointer) const {
	D_ASSERT(IsInPool(pointer));
	const auto offset = NumericCast<idx_t>(pointer - virtual_memory_space);
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
	return virtual_memory_space + NumericCast<idx_t>(block_id) * block_size;
}

data_ptr_t BlockAllocator::AllocateData(const idx_t size) const {
	if (!IsActive() || !enabled.load(std::memory_order_relaxed) || size != block_size) {
		return allocator.AllocateData(size);
	}
	return GetBlockAllocatorThreadLocalState(*this).Allocate();
}

void BlockAllocator::FreeData(const data_ptr_t pointer, const idx_t size) const {
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
	return (IsActive() && enabled.load(std::memory_order_relaxed)) || Allocator::SupportsFlush();
}

void BlockAllocator::ThreadFlush(bool allocator_background_threads, idx_t threshold, idx_t thread_count) const {
	GetBlockAllocatorThreadLocalState(*this).Clear();
	if (Allocator::SupportsFlush()) {
		Allocator::ThreadFlush(allocator_background_threads, threshold, thread_count);
	}
}

void BlockAllocator::FlushAll() const {
	FreeInternal();
	if (Allocator::SupportsFlush()) {
		Allocator::FlushAll();
	}
}

void BlockAllocator::TryFreeInternal() const {
	// Free many blocks in one go once we exceed the threshold
	if (to_free->q.size_approx() >= TO_FREE_SIZE_THRESHOLD) {
		FreeInternal();
	}
}

void BlockAllocator::FreeInternal() const {
	const unique_lock<mutex> guard(to_free_lock, std::try_to_lock);
	if (!guard.owns_lock()) {
		return;
	}

	uint32_t to_free_buffer[MAXIMUM_FREE_COUNT];
	do {
		const auto count = to_free->q.try_dequeue_bulk(to_free_buffer, MAXIMUM_FREE_COUNT);
		if (count == 0) {
			return;
		}

		// Sort so we can coalesce free calls
		std::sort(to_free_buffer, to_free_buffer + count);

		// Coalesce and free
		uint32_t block_id_start = to_free_buffer[0];
		for (idx_t i = 1; i < count; i++) {
			const auto &previous_block_id = to_free_buffer[i - 1];
			const auto &current_block_id = to_free_buffer[i];
			if (previous_block_id == current_block_id - 1) {
				continue; // Current is contiguous with previous block
			}

			// Previous block is the last contiguous block starting from block_id_start, free them in one go
			FreeContiguousBlocks(block_id_start, previous_block_id);

			// Continue coalescing from the current
			block_id_start = current_block_id;
		}

		// Don't forget the last one
		FreeContiguousBlocks(block_id_start, to_free_buffer[count - 1]);

		// Make freed blocks available to allocate again
		touched->q.enqueue_bulk(to_free_buffer, count);
	} while (to_free->q.size_approx() >= TO_FREE_SIZE_THRESHOLD);
}

void BlockAllocator::FreeContiguousBlocks(const uint32_t block_id_start, const uint32_t block_id_end_including) const {
	const auto pointer = GetPointer(block_id_start);
	const auto num_blocks = block_id_end_including - block_id_start + 1;
	const auto size = num_blocks * block_size;
	OnDeallocation(pointer, size);
}

} // namespace duckdb

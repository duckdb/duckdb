#include "duckdb/storage/block_allocator.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parallel/concurrentqueue.hpp"

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
	// Windows returns nullptr if the map fails
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
	// Incur page faults
	for (idx_t i = 0; i < size; i += 4096) {
		pointer[i] = 0;
	}
#endif
	if (!success) {
		throw InternalException("OnDeallocation failed");
	}
}

//===--------------------------------------------------------------------===//
// BlockAllocator
//===--------------------------------------------------------------------===//
struct BlockQueue {
	duckdb_moodycamel::ConcurrentQueue<uint32_t> q;
};

BlockAllocator::BlockAllocator(Allocator &allocator_p, const idx_t block_size_p, const idx_t virtual_memory_size_p)
    : allocator(allocator_p), block_size(block_size_p), block_size_div_shift(CountZeros<idx_t>::Trailing(block_size)),
      virtual_memory_size(AlignValue(virtual_memory_size_p, block_size)),
      virtual_memory_space(AllocateVirtualMemory(virtual_memory_size)), untouched(make_unsafe_uniq<BlockQueue>()),
      touched(make_unsafe_uniq<BlockQueue>()), to_free(make_unsafe_uniq<BlockQueue>()) {
	D_ASSERT(IsPowerOfTwo(block_size));
	Resize();
}

BlockAllocator::~BlockAllocator() {
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
	if (!IsActive() || size != block_size) {
		return allocator.AllocateData(size);
	}

	// Try to get a block ID. Reuse previous blocks if possible
	uint32_t block_id;
	if (to_free->q.try_dequeue(block_id)) {
		// NOP: we didn't free this one yet, can immediately reuse
	} else if (touched->q.try_dequeue(block_id)) {
		OnFirstAllocation(GetPointer(block_id), size);
	} else if (untouched->q.try_dequeue(block_id)) {
		// Nothing to do here
	} else {
		// We did not get a block ID, use fallback allocator
		return allocator.AllocateData(size);
	}

	return GetPointer(block_id);
}

void BlockAllocator::FreeData(const data_ptr_t pointer, const idx_t size) const {
	if (!IsActive() || !IsInPool(pointer)) {
		return allocator.FreeData(pointer, size);
	}
	D_ASSERT(size == block_size);

	// Add to queue to free later
	to_free->q.enqueue(GetBlockID(pointer));

	// Free many blocks in one go once we exceed the threshold
	if (to_free->q.size_approx() >= TO_FREE_SIZE_THRESHOLD) {
		FreeInternal();
	}
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

void BlockAllocator::FlushAll() const {
	FreeInternal();
	if (Allocator::SupportsFlush()) {
		Allocator::FlushAll();
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

			// Don't coalesce on Windows
#if !defined(_WIN32)
			if (previous_block_id == current_block_id - 1) {
				continue; // Current is contiguous with previous block
			}
#endif

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

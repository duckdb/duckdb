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
#if defined(_WIN32)
	if (!VirtualFree(pointer, 0, MEM_RELEASE)) {
		throw InternalException("FreeVirtualMemory failed");
	}
#else
	if (munmap(pointer, size) != 0) {
		throw InternalException("FreeVirtualMemory failed");
	}
#endif
}

enum class MemoryProtectionType { NO_ACCESS, ALLOW_READ_WRITE };

static void ProtectMemory(const data_ptr_t pointer, const idx_t size, const MemoryProtectionType type) {
#if defined(_WIN32)
	DWORD previous_flag;
	const auto flag = type == MemoryProtectionType::NO_ACCESS ? PAGE_NOACCESS : PAGE_READWRITE;
	if (!VirtualProtect(pointer, size, flag, &previous_flag)) {
		throw InternalException("ProtectMemory failed");
	}
#else
	const auto flag = type == MemoryProtectionType::NO_ACCESS ? PROT_NONE : PROT_READ | PROT_WRITE;
	if (mprotect(pointer, size, flag) != 0) {
		throw InternalException("ProtectMemory failed");
	}
#endif
}

static void ReturnMemory(const data_ptr_t pointer, const idx_t size) {
	// Tell the OS that it can lazily reclaim/zero these pages
#if defined(_WIN32)
	if (!VirtualAlloc(pointer, size, MEM_RESET, PAGE_READWRITE)) {
		throw InternalException("ReturnMemory failed");
	}
#else
	if (madvise(pointer, size, MADV_FREE) != 0) {
		throw InternalException("ReturnMemory failed");
	}
#endif
}

//===--------------------------------------------------------------------===//
// BlockAllocator
//===--------------------------------------------------------------------===//
typedef duckdb_moodycamel::ConcurrentQueue<uint32_t> block_queue_t;

struct BlockQueue {
	block_queue_t q;
};

BlockAllocator::BlockAllocator(Allocator &allocator_p, const idx_t block_size_p, const idx_t virtual_memory_size_p,
                               const idx_t physical_memory_size)
    : allocator(allocator_p), block_size(block_size_p), block_size_div_shift(CountZeros<idx_t>::Trailing(block_size)),
      virtual_memory_size(AlignValue(virtual_memory_size_p, block_size)),
      virtual_memory_space(AllocateVirtualMemory(virtual_memory_size)), physical_memory_size(0),
      untouched(make_unsafe_uniq<BlockQueue>()), touched(make_unsafe_uniq<BlockQueue>()) {
	D_ASSERT(IsPowerOfTwo(block_size));
	Resize(physical_memory_size);
}

BlockAllocator::~BlockAllocator() {
	FreeVirtualMemory(virtual_memory_space, virtual_memory_size);
}

BlockAllocator &BlockAllocator::Get(DatabaseInstance &db) {
	return *db.config.block_allocator;
}

BlockAllocator &BlockAllocator::Get(AttachedDatabase &db) {
	return Get(db.GetDatabase());
}

void BlockAllocator::Resize(const idx_t new_physical_memory_size) {
	lock_guard<mutex> guard(physical_memory_size_lock);
	if (new_physical_memory_size < physical_memory_size) {
		const string setting_name = BlockMemoryPoolSizeSetting::Name;
		throw InvalidInputException("%s cannot be reduced (current: %s)", setting_name,
		                            StringUtil::BytesToHumanReadableString(physical_memory_size));
	}

	// Determine start/end, then update physical memory size before enqueueing
	// This allows us to verify that block IDs are within the range with VerifyBlockID without the lock
	const auto start = NumericCast<uint32_t>(physical_memory_size / block_size);
	const auto end = NumericCast<uint32_t>(new_physical_memory_size / block_size);
	physical_memory_size = new_physical_memory_size;

	// Enqueue block IDs efficiently in batches
	uint32_t block_ids[STANDARD_VECTOR_SIZE];
	for (uint32_t block_id = start; block_id < end; block_id += STANDARD_VECTOR_SIZE) {
		const auto next = MinValue<idx_t>(end - block_id, STANDARD_VECTOR_SIZE);
		for (uint32_t i = 0; i < next; i++) {
			block_ids[i] = block_id + i;
		}
		untouched->q.enqueue_bulk(block_ids, next);
	}
}

bool BlockAllocator::IsActive() const {
	return physical_memory_size.load(std::memory_order_relaxed) != 0 && virtual_memory_space;
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
	const auto offset = UnsafeNumericCast<idx_t>(pointer - virtual_memory_space);
	D_ASSERT(ModuloBlockSize(offset) == 0);
	const auto block_id = UnsafeNumericCast<uint32_t>(DivBlockSize(offset));
	VerifyBlockID(block_id);
	return block_id;
}

void BlockAllocator::VerifyBlockID(const uint32_t block_id) const {
	D_ASSERT(block_id < NumericCast<uint32_t>(physical_memory_size / block_size));
}

data_ptr_t BlockAllocator::GetPointer(const uint32_t block_id) const {
	VerifyBlockID(block_id);
	return virtual_memory_space + UnsafeNumericCast<idx_t>(block_id) * block_size;
}

data_ptr_t BlockAllocator::AllocateData(const idx_t size) const {
	if (!IsActive() || size != block_size) {
		return allocator.AllocateData(size);
	}

	// Try to get a block ID. Reuse previously touched blocks first before getting an untouched block
	uint32_t block_id;
	if (!touched->q.try_dequeue(block_id) && !untouched->q.try_dequeue(block_id)) {
		// We did not get a block ID, use fallback allocator
		return allocator.AllocateData(size);
	}

	// Allow read/write on this block again
	// We don't need to do the inverse of "ReturnMemory", as the OS will lazily back this with physical memory again
	const auto pointer = GetPointer(block_id);
	ProtectMemory(pointer, size, MemoryProtectionType::ALLOW_READ_WRITE);
	return pointer;
}

void BlockAllocator::FreeData(const data_ptr_t pointer, const idx_t size) const {
	if (!IsActive() || !IsInPool(pointer)) {
		return allocator.FreeData(pointer, size);
	}
	D_ASSERT(size == block_size);

	// Return memory to OS before disallowing access, otherwise it's already inaccessible
	ReturnMemory(pointer, size);
	ProtectMemory(pointer, size, MemoryProtectionType::NO_ACCESS);

	// Add block ID to touched queue now
	touched->q.enqueue(GetBlockID(pointer));
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

} // namespace duckdb

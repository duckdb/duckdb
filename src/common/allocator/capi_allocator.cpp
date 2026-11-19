#include "duckdb/common/capi_allocator.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"

#ifdef DUCKDB_ENABLE_JEMALLOC
#include "jemalloc/jemalloc.h"
#endif

#include <cstddef>
#include <cstdlib>
#include <cstring>

namespace duckdb {

namespace {

// Each thread charges its own slot, and only moves it into the shared total once it holds this much.
constexpr int64_t FLUSH_THRESHOLD = 1 << 20;
constexpr idx_t SLOT_COUNT = 256;
// Apple silicon has 128-byte cache lines.
constexpr idx_t CACHE_LINE_SIZE = 128;

struct alignas(CACHE_LINE_SIZE) ChargeSlot {
	atomic<int64_t> pending {0};
};

//! Hands out slot indices to threads, reusing those of exited threads, so a slot only ever has one live writer.
class ThreadIndexRegistry {
public:
	optional_idx Acquire() {
		lock_guard<mutex> guard(lock);
		if (!free_indices.empty()) {
			auto index = free_indices.back();
			free_indices.pop_back();
			return index;
		}
		if (next_index < SLOT_COUNT) {
			return next_index++;
		}
		return optional_idx();
	}

	void Release(idx_t index) {
		lock_guard<mutex> guard(lock);
		free_indices.push_back(index);
	}

private:
	mutex lock;
	vector<idx_t> free_indices;
	idx_t next_index = 0;
};

struct CApiAllocatorState {
	ChargeSlot slots[SLOT_COUNT];
	//! Charged by threads without a slot of their own.
	ChargeSlot shared_slot;
	//! The bytes moved out of the slots, which each registered pool is charged.
	atomic<int64_t> flushed {0};
	mutex lock;
	vector<reference<BufferPool>> pools;
	ThreadIndexRegistry thread_indices;

	void Flush(int64_t delta) {
		if (delta == 0) {
			return;
		}
		lock_guard<mutex> guard(lock);
		flushed.store(flushed.load(std::memory_order_relaxed) + delta, std::memory_order_relaxed);
		for (auto &pool : pools) {
			pool.get().UpdateUsedMemory(MemoryTag::EXTENSION, delta);
		}
	}

	int64_t GetTotal() const {
		auto total = flushed.load(std::memory_order_relaxed) + shared_slot.pending.load(std::memory_order_relaxed);
		for (auto &slot : slots) {
			total += slot.pending.load(std::memory_order_relaxed);
		}
		return total;
	}
};

CApiAllocatorState &GetState() {
	// Leaked on purpose: C API memory may still be freed while static destructors run.
	static auto *state = new CApiAllocatorState();
	return *state;
}

// A thread's slot index plus one, or one of these.
constexpr idx_t SLOT_UNASSIGNED = 0;
constexpr idx_t SLOT_SHARED = NumericLimits<idx_t>::Maximum();

// Constant-initialized and trivially destructible, so reading it costs a single TLS lookup, and it stays readable while
// thread-local destructors of other libraries free memory.
thread_local idx_t thread_slot = SLOT_UNASSIGNED;

struct ThreadSlotRelease {
	~ThreadSlotRelease() {
		auto slot = thread_slot;
		thread_slot = SLOT_SHARED;
		if (slot != SLOT_UNASSIGNED && slot != SLOT_SHARED) {
			GetState().thread_indices.Release(slot - 1);
		}
	}
};

void AssignThreadSlot() {
	// Constructing it registers the destructor that hands the slot back when the thread exits.
	static thread_local ThreadSlotRelease release;
	auto index = GetState().thread_indices.Acquire();
	thread_slot = index.IsValid() ? index.GetIndex() + 1 : SLOT_SHARED;
}

void Charge(int64_t delta) {
	auto &state = GetState();
	auto slot_index = thread_slot;
	if (slot_index == SLOT_UNASSIGNED) {
		AssignThreadSlot();
		slot_index = thread_slot;
	}
	if (slot_index == SLOT_SHARED) {
		auto pending = state.shared_slot.pending.fetch_add(delta, std::memory_order_relaxed) + delta;
		if (AbsValue(pending) >= FLUSH_THRESHOLD) {
			state.Flush(state.shared_slot.pending.exchange(0, std::memory_order_relaxed));
		}
		return;
	}
	// Only this thread writes its slot, so it needs no read-modify-write.
	auto &slot = state.slots[slot_index - 1].pending;
	auto pending = slot.load(std::memory_order_relaxed) + delta;
	if (AbsValue(pending) >= FLUSH_THRESHOLD) {
		state.Flush(pending);
		pending = 0;
	}
	slot.store(pending, std::memory_order_relaxed);
}

// malloc aligns every allocation for any object that fits in it, up to this alignment, so rounding the size up to the
// alignment is enough below it. 64-bit x86 and ARM allocators all align to 16, even where max_align_t is smaller.
#if defined(__x86_64__) || defined(_M_X64) || defined(__aarch64__) || defined(_M_ARM64)
constexpr idx_t MALLOC_ALIGNMENT = MaxValue<idx_t>(16, alignof(std::max_align_t));
#else
constexpr idx_t MALLOC_ALIGNMENT = alignof(std::max_align_t);
#endif

bool IsOverAligned(idx_t alignment) {
	return alignment > MALLOC_ALIGNMENT;
}

[[noreturn]] void ThrowOutOfMemory(idx_t size, idx_t alignment) {
	throw OutOfMemoryException("Failed to allocate block of %llu bytes aligned to %llu (bad allocation)", size,
	                           alignment);
}

void CheckSize(idx_t size, idx_t alignment) {
	if (alignment >= Allocator::MAXIMUM_ALLOC_SIZE || size >= Allocator::MAXIMUM_ALLOC_SIZE - alignment) {
		ThrowOutOfMemory(size, alignment);
	}
}

#ifdef DUCKDB_ENABLE_JEMALLOC
// 0 asks for the default alignment, which covers every alignment that is not over-aligned.
int AlignmentFlags(idx_t alignment) {
	return IsOverAligned(alignment) ? MALLOCX_ALIGN(alignment) : 0;
}
#else
// Over-aligned allocations reserve `alignment` extra bytes: the aligned pointer is at most that far in, and the
// original pointer is stored right in front of it.
idx_t BackingSize(idx_t size, idx_t alignment) {
	return IsOverAligned(alignment) ? size + alignment : size;
}
#endif

} // namespace

data_ptr_t CApiAllocator::Allocate(idx_t size, idx_t alignment) {
	D_ASSERT(size > 0 && IsPowerOfTwo(alignment));
	CheckSize(size, alignment);
	size = MaxValue(size, alignment);
#ifdef DUCKDB_ENABLE_JEMALLOC
	auto result = data_ptr_cast(duckdb_je_mallocx(size, AlignmentFlags(alignment)));
	if (!result) {
		ThrowOutOfMemory(size, alignment);
	}
	Charge(NumericCast<int64_t>(size));
	return result;
#else
	auto backing_size = BackingSize(size, alignment);
	auto backing = data_ptr_cast(malloc(backing_size));
	if (!backing) {
		ThrowOutOfMemory(size, alignment);
	}
	Charge(NumericCast<int64_t>(backing_size));
	if (!IsOverAligned(alignment)) {
		return backing;
	}
	auto address = reinterpret_cast<uintptr_t>(backing) + sizeof(data_ptr_t);
	auto result = reinterpret_cast<data_ptr_t>(AlignValue<uintptr_t>(address, alignment));
	memcpy(result - sizeof(data_ptr_t), &backing, sizeof(data_ptr_t));
	return result;
#endif
}

void CApiAllocator::Free(data_ptr_t pointer, idx_t size, idx_t alignment) {
	D_ASSERT(pointer && size > 0 && IsPowerOfTwo(alignment));
	size = MaxValue(size, alignment);
#ifdef DUCKDB_ENABLE_JEMALLOC
	duckdb_je_sdallocx(pointer, size, AlignmentFlags(alignment));
	Charge(-NumericCast<int64_t>(size));
#else
	auto backing = pointer;
	if (IsOverAligned(alignment)) {
		memcpy(&backing, pointer - sizeof(data_ptr_t), sizeof(data_ptr_t));
	}
	free(backing);
	Charge(-NumericCast<int64_t>(BackingSize(size, alignment)));
#endif
}

data_ptr_t CApiAllocator::Reallocate(data_ptr_t pointer, idx_t old_size, idx_t alignment, idx_t new_size) {
	D_ASSERT(pointer && old_size > 0 && new_size > 0 && IsPowerOfTwo(alignment));
	CheckSize(new_size, alignment);
	old_size = MaxValue(old_size, alignment);
	new_size = MaxValue(new_size, alignment);
#ifdef DUCKDB_ENABLE_JEMALLOC
	auto result = data_ptr_cast(duckdb_je_rallocx(pointer, new_size, AlignmentFlags(alignment)));
	if (!result) {
		ThrowOutOfMemory(new_size, alignment);
	}
	Charge(NumericCast<int64_t>(new_size) - NumericCast<int64_t>(old_size));
	return result;
#else
	if (!IsOverAligned(alignment)) {
		auto result = data_ptr_cast(realloc(pointer, new_size));
		if (!result) {
			ThrowOutOfMemory(new_size, alignment);
		}
		Charge(NumericCast<int64_t>(new_size) - NumericCast<int64_t>(old_size));
		return result;
	}
	// realloc does not keep the alignment, so move the data to a fresh allocation
	auto result = Allocate(new_size, alignment);
	memcpy(result, pointer, MinValue(old_size, new_size));
	Free(pointer, old_size, alignment);
	return result;
#endif
}

idx_t CApiAllocator::GetAllocatedSize() {
	auto allocated = GetState().GetTotal();
	return allocated > 0 ? NumericCast<idx_t>(allocated) : 0;
}

void CApiAllocator::RegisterPool(BufferPool &pool) {
	auto &state = GetState();
	lock_guard<mutex> guard(state.lock);
	pool.UpdateUsedMemory(MemoryTag::EXTENSION, state.flushed.load(std::memory_order_relaxed));
	state.pools.push_back(pool);
}

void CApiAllocator::UnregisterPool(BufferPool &pool) {
	auto &state = GetState();
	lock_guard<mutex> guard(state.lock);
	for (idx_t i = 0; i < state.pools.size(); i++) {
		if (&state.pools[i].get() == &pool) {
			state.pools.erase_at(i);
			return;
		}
	}
	D_ASSERT(false);
}

} // namespace duckdb

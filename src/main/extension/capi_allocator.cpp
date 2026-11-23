#include "duckdb/main/extension/capi_allocator.hpp"

#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <cstddef>
#include <cstring>

#ifdef DUCKDB_ENABLE_JEMALLOC
#include "jemalloc/jemalloc.h"
#endif

namespace duckdb {

namespace {

constexpr idx_t SLOT_COUNT = 256;
#if defined(__x86_64__) || defined(_M_X64)
constexpr idx_t CACHE_LINE_SIZE = 64;
#else
// Apple silicon has 128-byte cache lines.
constexpr idx_t CACHE_LINE_SIZE = 128;
#endif

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

	static ThreadIndexRegistry &Get() {
		// Leaked on purpose: memory may still be freed while static destructors run.
		static auto *registry = new ThreadIndexRegistry();
		return *registry;
	}

private:
	ThreadIndexRegistry() {
		// Release runs in thread-local destructors, and must not fail by allocating.
		free_indices.reserve(SLOT_COUNT);
	}

	mutex lock;
	vector<idx_t> free_indices;
	idx_t next_index = 0;
};

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
			ThreadIndexRegistry::Get().Release(slot - 1);
		}
	}
};

idx_t AssignThreadSlot() {
	// Constructing it registers the destructor that hands the slot back when the thread exits.
	static thread_local ThreadSlotRelease release;
	auto index = ThreadIndexRegistry::Get().Acquire();
	thread_slot = index.IsValid() ? index.GetIndex() + 1 : SLOT_SHARED;
	return thread_slot;
}

//! The bytes held by one allocator. Each thread charges its own slot, so charging needs no read-modify-write and
//! threads never contend; reading sums the slots.
class ChargeCounter {
public:
	void Add(int64_t delta) {
		auto slot = thread_slot;
		if (DUCKDB_UNLIKELY(slot == SLOT_UNASSIGNED)) {
			slot = AssignThreadSlot();
		}
		// SLOT_SHARED wraps to an index past the slots.
		auto index = slot - 1;
		if (DUCKDB_UNLIKELY(index >= SLOT_COUNT)) {
			shared.fetch_add(delta, std::memory_order_relaxed);
			return;
		}
		auto &value = slots[index].value;
		value.store(value.load(std::memory_order_relaxed) + delta, std::memory_order_relaxed);
	}

	int64_t Load() const {
		auto total = shared.load(std::memory_order_relaxed);
		for (auto &slot : slots) {
			total += slot.value.load(std::memory_order_relaxed);
		}
		return total;
	}

private:
	struct alignas(CACHE_LINE_SIZE) Slot {
		atomic<int64_t> value {0};
	};
	Slot slots[SLOT_COUNT];
	//! Charged by threads that found no free slot.
	alignas(CACHE_LINE_SIZE) atomic<int64_t> shared {0};
};

struct ExtensionCounters {
	mutex lock;
	unordered_map<string, reference_set_t<ChargeCounter>> by_name;

	static shared_ptr<ExtensionCounters> Get() {
		static auto registry = make_shared_ptr<ExtensionCounters>();
		return registry;
	}
};

struct ScopedAllocatorData : PrivateAllocatorData {
	explicit ScopedAllocatorData(bool process_global_p) : process_global(process_global_p) {
	}

	//! Whether this is an ExtensionAllocatorData, checked without a virtual call on every allocation.
	const bool process_global;

	virtual void Register(const string &name) {
		throw InvalidInputException("Cannot register a database-scoped allocator with an extension");
	}
};

struct ExtensionAllocatorData : ScopedAllocatorData {
	ExtensionAllocatorData() : ScopedAllocatorData(true) {
	}

	ChargeCounter bytes;
	shared_ptr<ExtensionCounters> registry;
	string extension_name;

	void Register(const string &name) override {
		auto counters = ExtensionCounters::Get();
		lock_guard<mutex> guard(counters->lock);
		if (registry) {
			if (extension_name != name) {
				throw InvalidInputException("Allocator is already registered to extension '%s'", extension_name);
			}
			return;
		}
		extension_name = name;
		counters->by_name[name].insert(bytes);
		registry = std::move(counters);
	}

	~ExtensionAllocatorData() override {
		if (registry) {
			lock_guard<mutex> guard(registry->lock);
			registry->by_name.at(extension_name).erase(bytes);
		}
	}

	static data_ptr_t Allocate(PrivateAllocatorData *data, idx_t size) {
		auto ptr = Allocator::DefaultAllocate(data, size);
		if (ptr) {
			data->Cast<ExtensionAllocatorData>().bytes.Add(NumericCast<int64_t>(size));
		}
		return ptr;
	}

	static void Free(PrivateAllocatorData *data, data_ptr_t ptr, idx_t size) {
		Allocator::DefaultFree(data, ptr, size);
		data->Cast<ExtensionAllocatorData>().bytes.Add(-NumericCast<int64_t>(size));
	}

	static data_ptr_t Reallocate(PrivateAllocatorData *data, data_ptr_t ptr, idx_t old_size, idx_t size) {
		auto result = Allocator::DefaultReallocate(data, ptr, old_size, size);
		if (result) {
			data->Cast<ExtensionAllocatorData>().bytes.Add(int64_t(size) - int64_t(old_size));
		}
		return result;
	}
};

struct DatabaseAllocatorData : ScopedAllocatorData {
	explicit DatabaseAllocatorData(DatabaseInstance &instance)
	    : ScopedAllocatorData(false), db(instance.shared_from_this()), allocator(Allocator::Get(instance)),
	      manager(BufferManager::GetBufferManager(instance)) {
	}

	shared_ptr<DatabaseInstance> db;
	Allocator &allocator;
	BufferManager &manager;

	static data_ptr_t Allocate(PrivateAllocatorData *data, idx_t size) {
		return Reallocate(data, nullptr, 0, size);
	}

	static void Free(PrivateAllocatorData *data, data_ptr_t ptr, idx_t size) {
		auto &state = data->Cast<DatabaseAllocatorData>();
		state.allocator.FreeData(ptr, size);
		state.manager.FreeReservedMemory(size);
	}

	static data_ptr_t Reallocate(PrivateAllocatorData *data, data_ptr_t ptr, idx_t old_size, idx_t size) {
		auto &state = data->Cast<DatabaseAllocatorData>();
		if (size > old_size) {
			state.manager.ReserveMemory(size - old_size);
		}
		data_ptr_t result;
		try {
			result = ptr ? state.allocator.ReallocateData(ptr, old_size, size) : state.allocator.AllocateData(size);
		} catch (...) {
			if (size > old_size) {
				state.manager.FreeReservedMemory(size - old_size);
			}
			throw;
		}
		if (size < old_size) {
			state.manager.FreeReservedMemory(old_size - size);
		}
		return result;
	}
};

// Allocators align every block to this much, as long as the block is at least that large.
constexpr idx_t DEFAULT_ALIGNMENT = alignof(std::max_align_t);

bool IsOverAligned(idx_t alignment) {
	return alignment > DEFAULT_ALIGNMENT;
}

[[noreturn]] void ThrowOutOfMemory(idx_t size, idx_t alignment) {
	throw OutOfMemoryException("Failed to allocate block of %llu bytes aligned to %llu (bad allocation)", size,
	                           alignment);
}

// Over-aligned blocks reserve `alignment` extra bytes: the aligned pointer is at most that far in, and the backing
// pointer is stored right in front of it. Smaller blocks are rounded up to their alignment instead.
idx_t BackingSize(idx_t size, idx_t alignment) {
	const auto limit = MinValue<idx_t>(Allocator::MAXIMUM_ALLOC_SIZE, NumericLimits<uintptr_t>::Maximum());
	if (alignment >= limit || size >= limit - alignment) {
		ThrowOutOfMemory(size, alignment);
	}
	return IsOverAligned(alignment) ? size + alignment : MaxValue(size, alignment);
}

data_ptr_t AlignBacking(data_ptr_t backing, idx_t alignment) {
	auto address = reinterpret_cast<uintptr_t>(backing + sizeof(data_ptr_t));
	return reinterpret_cast<data_ptr_t>(AlignValue<uintptr_t>(address, alignment));
}

data_ptr_t StoreBacking(data_ptr_t backing, data_ptr_t aligned) {
	Store<data_ptr_t>(backing, aligned - sizeof(data_ptr_t));
	return aligned;
}

data_ptr_t LoadBacking(data_ptr_t pointer, idx_t alignment) {
	return IsOverAligned(alignment) ? Load<data_ptr_t>(pointer - sizeof(data_ptr_t)) : pointer;
}

//! Aligns blocks of any allocator by over-allocating.
struct PaddedAllocation {
	static data_ptr_t Allocate(Allocator &allocator, idx_t size, idx_t alignment, bool zeroed) {
		auto backing = allocator.AllocateData(BackingSize(size, alignment));
		auto result = IsOverAligned(alignment) ? StoreBacking(backing, AlignBacking(backing, alignment)) : backing;
		if (zeroed) {
			memset(result, 0, size);
		}
		return result;
	}

	static void Free(Allocator &allocator, data_ptr_t pointer, idx_t size, idx_t alignment) {
		allocator.FreeData(LoadBacking(pointer, alignment), BackingSize(size, alignment));
	}

	static data_ptr_t Reallocate(Allocator &allocator, data_ptr_t pointer, idx_t old_size, idx_t alignment,
	                             idx_t new_size) {
		auto backing = LoadBacking(pointer, alignment);
		auto old_offset = idx_t(pointer - backing);
		backing = allocator.ReallocateData(backing, BackingSize(old_size, alignment), BackingSize(new_size, alignment));
		if (!IsOverAligned(alignment)) {
			return backing;
		}
		// realloc preserves the backing bytes, but may move the payload to a different alignment offset. Move it before
		// storing the backing pointer, which can overlap where the payload was.
		auto aligned = AlignBacking(backing, alignment);
		if (aligned != backing + old_offset) {
			memmove(aligned, backing + old_offset, MinValue(old_size, new_size));
		}
		return StoreBacking(backing, aligned);
	}
};

#ifdef DUCKDB_ENABLE_JEMALLOC
//! Process-global blocks straight from jemalloc, which aligns them itself and frees them by size.
struct JemallocAllocation {
	static bool FitsLimit(idx_t size, idx_t alignment) {
		return alignment < Allocator::MAXIMUM_ALLOC_SIZE && size < Allocator::MAXIMUM_ALLOC_SIZE - alignment;
	}

	// Rounding the size up to the alignment gives every block that is not over-aligned its alignment for free.
	static idx_t BlockSize(idx_t size, idx_t alignment) {
		return MaxValue(size, alignment);
	}

	// MALLOCX_ALIGN calls ffs through the PLT.
	static int Flags(idx_t alignment) {
		return IsOverAligned(alignment) ? MALLOCX_LG_ALIGN(CountZeros<uint64_t>::Trailing(alignment)) : 0;
	}

	static void *TryAllocate(ExtensionAllocatorData &data, idx_t size, idx_t alignment, bool zeroed) noexcept {
		if (DUCKDB_UNLIKELY(!FitsLimit(size, alignment))) {
			return nullptr;
		}
		auto block_size = BlockSize(size, alignment);
		void *result;
		if (IsOverAligned(alignment) || (zeroed && block_size >= LARGE_BLOCK_SIZE)) {
			// jemalloc can hand out large blocks that are zero already instead of clearing them.
			result = duckdb_je_mallocx(block_size, Flags(alignment) | (zeroed ? MALLOCX_ZERO : 0));
		} else {
			// malloc has a fast path that mallocx lacks.
			result = duckdb_je_malloc(block_size);
			if (zeroed && result) {
				memset(result, 0, block_size);
			}
		}
		if (DUCKDB_LIKELY(result != nullptr)) {
			data.bytes.Add(static_cast<int64_t>(block_size));
		}
		return result;
	}

	static void Free(ExtensionAllocatorData &data, void *pointer, idx_t size, idx_t alignment) noexcept {
		auto block_size = BlockSize(size, alignment);
		duckdb_je_sdallocx(pointer, block_size, Flags(alignment));
		data.bytes.Add(-static_cast<int64_t>(block_size));
	}

	static void *TryReallocate(ExtensionAllocatorData &data, void *pointer, idx_t old_size, idx_t alignment,
	                           idx_t new_size) noexcept {
		if (DUCKDB_UNLIKELY(!FitsLimit(new_size, alignment))) {
			return nullptr;
		}
		auto old_block_size = BlockSize(old_size, alignment);
		auto new_block_size = BlockSize(new_size, alignment);
		auto result = duckdb_je_rallocx(pointer, new_block_size, Flags(alignment));
		if (DUCKDB_LIKELY(result != nullptr)) {
			data.bytes.Add(static_cast<int64_t>(new_block_size) - static_cast<int64_t>(old_block_size));
		}
		return result;
	}

	static data_ptr_t Allocate(ExtensionAllocatorData &data, idx_t size, idx_t alignment, bool zeroed) {
		auto result = TryAllocate(data, size, alignment, zeroed);
		if (!result) {
			ThrowOutOfMemory(size, alignment);
		}
		return data_ptr_cast(result);
	}

	static data_ptr_t Reallocate(ExtensionAllocatorData &data, data_ptr_t pointer, idx_t old_size, idx_t alignment,
	                             idx_t new_size) {
		auto result = TryReallocate(data, pointer, old_size, alignment, new_size);
		if (!result) {
			ThrowOutOfMemory(new_size, alignment);
		}
		return data_ptr_cast(result);
	}

	// The state of the raw functions is the allocator data itself.
	static void *RawAllocate(void *state, idx_t size, idx_t alignment) noexcept {
		return TryAllocate(*static_cast<ExtensionAllocatorData *>(state), size, alignment, false);
	}

	static void *RawAllocateZeroed(void *state, idx_t size, idx_t alignment) noexcept {
		return TryAllocate(*static_cast<ExtensionAllocatorData *>(state), size, alignment, true);
	}

	static void RawFree(void *state, void *pointer, idx_t size, idx_t alignment) noexcept {
		Free(*static_cast<ExtensionAllocatorData *>(state), pointer, size, alignment);
	}

	static void *RawReallocate(void *state, void *pointer, idx_t old_size, idx_t alignment, idx_t new_size) noexcept {
		return TryReallocate(*static_cast<ExtensionAllocatorData *>(state), pointer, old_size, alignment, new_size);
	}

private:
	//! The smallest size jemalloc serves from its own extents rather than from a slab.
	static constexpr idx_t LARGE_BLOCK_SIZE = 16384;
};
#endif

ScopedAllocatorData &GetData(Allocator &allocator) {
	return allocator.GetPrivateData()->Cast<ScopedAllocatorData>();
}

//! Raw functions for allocators without a jemalloc fast path. Their state is the Allocator.
struct CheckedRawFunctions {
	static void *Allocate(void *state, idx_t size, idx_t alignment) noexcept {
		try {
			return ExtensionAllocator::Allocate(*static_cast<Allocator *>(state), size, alignment, false);
		} catch (...) {
			return nullptr;
		}
	}

	static void *AllocateZeroed(void *state, idx_t size, idx_t alignment) noexcept {
		try {
			return ExtensionAllocator::Allocate(*static_cast<Allocator *>(state), size, alignment, true);
		} catch (...) {
			return nullptr;
		}
	}

	static void Free(void *state, void *pointer, idx_t size, idx_t alignment) noexcept {
		try {
			ExtensionAllocator::Free(*static_cast<Allocator *>(state), static_cast<data_ptr_t>(pointer), size,
			                         alignment);
		} catch (...) { // NOLINT(bugprone-empty-catch)
		}
	}

	static void *Reallocate(void *state, void *pointer, idx_t old_size, idx_t alignment, idx_t new_size) noexcept {
		try {
			return ExtensionAllocator::Reallocate(*static_cast<Allocator *>(state), static_cast<data_ptr_t>(pointer),
			                                      old_size, alignment, new_size);
		} catch (...) {
			return nullptr;
		}
	}
};

} // namespace

unique_ptr<Allocator> ExtensionAllocator::Create() {
	return make_uniq<Allocator>(ExtensionAllocatorData::Allocate, ExtensionAllocatorData::Free,
	                            ExtensionAllocatorData::Reallocate, make_uniq<ExtensionAllocatorData>());
}

unique_ptr<Allocator> ExtensionAllocator::Create(DatabaseInstance &db) {
	return make_uniq<Allocator>(DatabaseAllocatorData::Allocate, DatabaseAllocatorData::Free,
	                            DatabaseAllocatorData::Reallocate, make_uniq<DatabaseAllocatorData>(db));
}

void ExtensionAllocator::Register(Allocator &allocator, const string &extension_name) {
	allocator.GetPrivateData()->Cast<ScopedAllocatorData>().Register(extension_name);
}

optional_idx ExtensionAllocator::GetUsage(const string &extension_name) {
	auto registry = ExtensionCounters::Get();
	lock_guard<mutex> guard(registry->lock);
	auto it = registry->by_name.find(extension_name);
	if (it == registry->by_name.end()) {
		return optional_idx();
	}
	int64_t total = 0;
	for (auto &bytes : it->second) {
		total += bytes.get().Load();
	}
	// Slots are read one by one, so a block freed on another thread while reading can make the sum dip below zero.
	return NumericCast<idx_t>(MaxValue<int64_t>(total, 0));
}

data_ptr_t ExtensionAllocator::Allocate(Allocator &allocator, idx_t size, idx_t alignment, bool zeroed) {
#ifdef DUCKDB_ENABLE_JEMALLOC
	auto &data = GetData(allocator);
	if (data.process_global) {
		return JemallocAllocation::Allocate(data.Cast<ExtensionAllocatorData>(), size, alignment, zeroed);
	}
#endif
	return PaddedAllocation::Allocate(allocator, size, alignment, zeroed);
}

void ExtensionAllocator::Free(Allocator &allocator, data_ptr_t pointer, idx_t size, idx_t alignment) {
#ifdef DUCKDB_ENABLE_JEMALLOC
	auto &data = GetData(allocator);
	if (data.process_global) {
		return JemallocAllocation::Free(data.Cast<ExtensionAllocatorData>(), pointer, size, alignment);
	}
#endif
	PaddedAllocation::Free(allocator, pointer, size, alignment);
}

data_ptr_t ExtensionAllocator::Reallocate(Allocator &allocator, data_ptr_t pointer, idx_t old_size, idx_t alignment,
                                          idx_t new_size) {
#ifdef DUCKDB_ENABLE_JEMALLOC
	auto &data = GetData(allocator);
	if (data.process_global) {
		return JemallocAllocation::Reallocate(data.Cast<ExtensionAllocatorData>(), pointer, old_size, alignment,
		                                      new_size);
	}
#endif
	return PaddedAllocation::Reallocate(allocator, pointer, old_size, alignment, new_size);
}

ExtensionAllocator::RawFunctions ExtensionAllocator::GetRawFunctions(Allocator &allocator) {
#ifdef DUCKDB_ENABLE_JEMALLOC
	auto &data = GetData(allocator);
	if (data.process_global) {
		return {&data, JemallocAllocation::RawAllocate, JemallocAllocation::RawAllocateZeroed,
		        JemallocAllocation::RawFree, JemallocAllocation::RawReallocate};
	}
#endif
	return {&allocator, CheckedRawFunctions::Allocate, CheckedRawFunctions::AllocateZeroed, CheckedRawFunctions::Free,
	        CheckedRawFunctions::Reallocate};
}

} // namespace duckdb

#include "duckdb/main/extension/capi_allocator.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"

#include <cstddef>
#include <cstring>

namespace duckdb {

namespace {

// Charges to the buffer pools are batched, so the lock is only taken once the held bytes moved this far.
constexpr int64_t PUBLISH_THRESHOLD = 64 * 1024;

struct PoolCharge {
	shared_ptr<BufferPool> pool;
	idx_t attach_count;
};

} // namespace

struct CApiAllocatorData : public PrivateAllocatorData {
	atomic<int64_t> allocated {0};
	//! The bytes each attached pool is currently charged.
	atomic<int64_t> published {0};
	mutex lock;
	vector<PoolCharge> pools;

	void Charge(int64_t delta) {
		auto total = allocated.fetch_add(delta, std::memory_order_relaxed) + delta;
		if (AbsValue(total - published.load(std::memory_order_relaxed)) >= PUBLISH_THRESHOLD) {
			Publish();
		}
	}

	void Publish() {
		lock_guard<mutex> guard(lock);
		auto total = allocated.load(std::memory_order_relaxed);
		auto delta = total - published.load(std::memory_order_relaxed);
		if (delta == 0) {
			return;
		}
		for (auto &entry : pools) {
			entry.pool->UpdateUsedMemory(MemoryTag::EXTENSION, delta);
		}
		published.store(total, std::memory_order_relaxed);
	}

	void Attach(shared_ptr<BufferPool> pool) {
		lock_guard<mutex> guard(lock);
		for (auto &entry : pools) {
			if (entry.pool == pool) {
				entry.attach_count++;
				return;
			}
		}
		pool->UpdateUsedMemory(MemoryTag::EXTENSION, published.load(std::memory_order_relaxed));
		pools.push_back(PoolCharge {std::move(pool), 1});
	}

	void Detach(BufferPool &pool) {
		lock_guard<mutex> guard(lock);
		for (idx_t i = 0; i < pools.size(); i++) {
			auto &entry = pools[i];
			if (entry.pool.get() != &pool) {
				continue;
			}
			if (--entry.attach_count == 0) {
				pool.UpdateUsedMemory(MemoryTag::EXTENSION, -published.load(std::memory_order_relaxed));
				pools.erase_at(i);
			}
			return;
		}
		D_ASSERT(false);
	}
};

namespace {

CApiAllocatorData &GetData(PrivateAllocatorData *data) {
	return data->Cast<CApiAllocatorData>();
}

data_ptr_t CApiAllocate(PrivateAllocatorData *data, idx_t size) {
	auto result = Allocator::DefaultAllocate(data, size);
	if (result) {
		GetData(data).Charge(NumericCast<int64_t>(size));
	}
	return result;
}

void CApiFree(PrivateAllocatorData *data, data_ptr_t pointer, idx_t size) {
	Allocator::DefaultFree(data, pointer, size);
	GetData(data).Charge(-NumericCast<int64_t>(size));
}

data_ptr_t CApiReallocate(PrivateAllocatorData *data, data_ptr_t pointer, idx_t old_size, idx_t size) {
	auto result = Allocator::DefaultReallocate(data, pointer, old_size, size);
	if (result) {
		GetData(data).Charge(NumericCast<int64_t>(size) - NumericCast<int64_t>(old_size));
	}
	return result;
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

// Over-aligned allocations reserve `alignment` extra bytes: the aligned pointer is at most that far in, and the
// original pointer is stored right in front of it.
idx_t BackingSize(idx_t size, idx_t alignment) {
	if (!IsOverAligned(alignment)) {
		return MaxValue(size, alignment);
	}
	return size + alignment;
}

void CheckSize(idx_t size, idx_t alignment) {
	if (alignment >= Allocator::MAXIMUM_ALLOC_SIZE || size >= Allocator::MAXIMUM_ALLOC_SIZE - alignment) {
		throw OutOfMemoryException("Failed to allocate block of %llu bytes aligned to %llu (bad allocation)", size,
		                           alignment);
	}
}

} // namespace

CApiAllocator::CApiAllocator(string extension_name_p)
    : Allocator(CApiAllocate, CApiFree, CApiReallocate, make_uniq<CApiAllocatorData>()),
      data(GetData(GetPrivateData())), extension_name(std::move(extension_name_p)) {
}

CApiAllocator &CApiAllocator::Get(const string &extension_name) {
	// Leaked on purpose: extension memory may still be freed while static destructors run.
	static auto *registry_lock = new mutex();
	static auto *registry = new unordered_map<string, unique_ptr<CApiAllocator>>();

	lock_guard<mutex> guard(*registry_lock);
	auto &entry = (*registry)[extension_name];
	if (!entry) {
		entry = unique_ptr<CApiAllocator>(new CApiAllocator(extension_name));
	}
	return *entry;
}

idx_t CApiAllocator::GetAllocatedSize() const {
	auto allocated = data.allocated.load(std::memory_order_relaxed);
	return allocated > 0 ? NumericCast<idx_t>(allocated) : 0;
}

data_ptr_t CApiAllocator::AllocateAligned(idx_t size, idx_t alignment) {
	D_ASSERT(size > 0 && IsPowerOfTwo(alignment));
	CheckSize(size, alignment);
	auto backing = AllocateData(BackingSize(size, alignment));
	if (!IsOverAligned(alignment)) {
		return backing;
	}
	auto address = reinterpret_cast<uintptr_t>(backing) + sizeof(data_ptr_t);
	auto result = reinterpret_cast<data_ptr_t>(AlignValue<uintptr_t>(address, alignment));
	memcpy(result - sizeof(data_ptr_t), &backing, sizeof(data_ptr_t));
	return result;
}

void CApiAllocator::FreeAligned(data_ptr_t pointer, idx_t size, idx_t alignment) {
	D_ASSERT(pointer && size > 0 && IsPowerOfTwo(alignment));
	auto backing = pointer;
	if (IsOverAligned(alignment)) {
		memcpy(&backing, pointer - sizeof(data_ptr_t), sizeof(data_ptr_t));
	}
	FreeData(backing, BackingSize(size, alignment));
}

data_ptr_t CApiAllocator::ReallocateAligned(data_ptr_t pointer, idx_t old_size, idx_t alignment, idx_t new_size) {
	D_ASSERT(pointer && old_size > 0 && new_size > 0 && IsPowerOfTwo(alignment));
	CheckSize(new_size, alignment);
	if (!IsOverAligned(alignment)) {
		return ReallocateData(pointer, BackingSize(old_size, alignment), BackingSize(new_size, alignment));
	}
	// realloc does not keep the alignment, so move the data to a fresh allocation
	auto result = AllocateAligned(new_size, alignment);
	memcpy(result, pointer, MinValue(old_size, new_size));
	FreeAligned(pointer, old_size, alignment);
	return result;
}

void CApiAllocator::Attach(shared_ptr<BufferPool> pool) {
	data.Attach(std::move(pool));
}

void CApiAllocator::Detach(BufferPool &pool) {
	data.Detach(pool);
}

CApiAllocatorAttachment::CApiAllocatorAttachment(CApiAllocator &allocator, shared_ptr<BufferPool> pool_p)
    : allocator(allocator), pool(std::move(pool_p)) {
	allocator.Attach(pool);
}

CApiAllocatorAttachment::~CApiAllocatorAttachment() {
	allocator.Detach(*pool);
}

} // namespace duckdb

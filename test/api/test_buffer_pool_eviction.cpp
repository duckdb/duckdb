#include "catch.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

namespace {
// Constants used in buffer pool memory limit setting.
constexpr const char *EXCEPTION_POSTSCRIPT = "exception postscript";

struct NonEvictableObject : public ObjectCacheEntry {
	int value;
	explicit NonEvictableObject(int value_p) : value(value_p) {
	}
	~NonEvictableObject() override = default;
	string GetObjectType() override {
		return ObjectType();
	}
	static string ObjectType() {
		return "NonEvictableTestObject";
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx {};
	}
};
struct EvictableTestObject : public ObjectCacheEntry {
	int value;
	idx_t size;
	EvictableTestObject(int value_p, idx_t size_p) : value(value_p), size(size_p) {
	}
	~EvictableTestObject() override = default;
	string GetObjectType() override {
		return ObjectType();
	}
	static string ObjectType() {
		return "EvictableTestObject";
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx(size);
	}
};
} // namespace

TEST_CASE("Test buffer pool eviction: pages before object cache", "[storage][buffer_pool]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();
	auto &cache = ObjectCache::GetObjectCache(context);
	const idx_t initial_memory = buffer_pool.GetUsedMemory();

	// Set a memory limit that will force eviction
	constexpr idx_t page_size = 1024 * 1024; // 1MiB per page
	constexpr idx_t obj_size = 1024 * 1024;  // 1MiB per object cache entry
	constexpr idx_t num_pages = 5;
	constexpr idx_t num_objects = 3;
	const idx_t actual_page_alloc_size = BufferManager::GetAllocSize(page_size + Storage::DEFAULT_BLOCK_HEADER_SIZE);

	// Set limit to hold all objects and some pages, but not all pages with extra allocation
	const idx_t memory_limit = (num_pages - 2) * actual_page_alloc_size + num_objects * obj_size;
	buffer_pool.SetLimit(memory_limit, EXCEPTION_POSTSCRIPT);

	// Add object cache entries first
	for (idx_t idx = 0; idx < num_objects; ++idx) {
		cache.Put(StringUtil::Format("obj%llu", idx), make_shared_ptr<EvictableTestObject>(idx, obj_size));
	}
	const idx_t after_objects_memory = buffer_pool.GetUsedMemory();
	REQUIRE(after_objects_memory == initial_memory + num_objects * obj_size);
	REQUIRE(cache.GetEntryCount() == num_objects);

	// Add unpinned pages, which could be be added to the eviction queue and evicted later
	vector<shared_ptr<BlockHandle>> pages;
	pages.reserve(num_pages);
	for (idx_t idx = 0; idx < num_pages; ++idx) {
		const auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, page_size, /*can_destroy=*/true);
		pages.emplace_back(pin.GetBlockHandle());
		// Pin is destroyed, so page is added to eviction queue
	}

	// Verify all object cache entries are still present, since pages are evicted first
	REQUIRE(cache.GetEntryCount() == num_objects);
	for (idx_t idx = 0; idx < num_objects; ++idx) {
		auto obj = cache.GetObject(StringUtil::Format("obj%llu", idx));
		REQUIRE(obj != nullptr);
	}

	// Check overall memory usage is equal to memory limit.
	const auto final_memory_usage = buffer_manager.GetUsedMemory();
	REQUIRE(final_memory_usage == memory_limit);
}

TEST_CASE("Test buffer pool eviction: pinned pages can evict object cache", "[storage][buffer_pool]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();
	auto &cache = ObjectCache::GetObjectCache(context);
	const idx_t initial_memory = buffer_pool.GetUsedMemory();

	// Set a memory limit that will force eviction
	constexpr idx_t page_size = 1024 * 1024; // 1MiB per page
	constexpr idx_t obj_size = 1024 * 1024;  // 1MiB per object cache entry
	constexpr idx_t num_objects = 5;
	constexpr idx_t num_pages = 6;
	const idx_t actual_page_alloc_size = BufferManager::GetAllocSize(page_size + Storage::DEFAULT_BLOCK_HEADER_SIZE);

	// Set limit to hold all pages, some objects, and initial overhead
	const idx_t after_eviction_memory = (num_objects - 2) * obj_size + num_pages * actual_page_alloc_size;
	const idx_t total_memory_limit = initial_memory + after_eviction_memory;
	buffer_pool.SetLimit(total_memory_limit, EXCEPTION_POSTSCRIPT);

	// Add object cache entries first
	for (idx_t idx = 0; idx < num_objects; ++idx) {
		cache.Put(StringUtil::Format("obj%llu", idx), make_shared_ptr<EvictableTestObject>(idx, obj_size));
	}
	const idx_t after_objects_memory = buffer_pool.GetUsedMemory();
	REQUIRE(after_objects_memory == initial_memory + num_objects * obj_size);
	REQUIRE(cache.GetEntryCount() == num_objects);

	// Now pin many pages, which makes sure the eviction of object cache entries
	vector<BufferHandle> pinned_pages;
	pinned_pages.reserve(num_pages);
	for (idx_t idx = 0; idx < num_pages; ++idx) {
		// If allocation exceeds memory limit, object cache entries will be evicted first.
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, page_size, /*can_destroy=*/true);
		pinned_pages.emplace_back(std::move(pin));
	}

	// Check object cache entries are partially evicted.
	vector<idx_t> evicted_entries;
	for (idx_t idx = 0; idx < num_objects; ++idx) {
		auto obj = cache.GetObject(StringUtil::Format("obj%llu", idx));
		if (obj == nullptr) {
			evicted_entries.emplace_back(idx);
		}
	}
	// Check some of the cache entries have been evicted, and eviction is performed in the order of insertion.
	REQUIRE(evicted_entries == vector<idx_t> {0, 1});

	// Check overall memory usage is equal to memory limit.
	const auto final_memory_usage = buffer_manager.GetUsedMemory();
	REQUIRE(final_memory_usage == total_memory_limit);
}

TEST_CASE("Test buffer pool eviction: non-evictable objects are kept", "[storage][buffer_pool]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();
	auto &cache = ObjectCache::GetObjectCache(context);
	const idx_t initial_memory = buffer_pool.GetUsedMemory();

	// Set a memory limit that will force eviction
	constexpr idx_t page_size = 1024 * 1024; // 1MiB per page
	constexpr idx_t obj_size = 1024 * 1024;  // 1MiB per object cache entry
	constexpr idx_t num_non_evictable_objects = 1;
	constexpr idx_t num_evictable_objects = 4;
	constexpr idx_t num_objects = num_non_evictable_objects + num_evictable_objects;
	constexpr idx_t num_pages = 6;
	const idx_t actual_page_alloc_size = BufferManager::GetAllocSize(page_size + Storage::DEFAULT_BLOCK_HEADER_SIZE);

	// Set limit to hold all pages, some objects, and initial overhead
	const idx_t after_eviction_memory = (num_evictable_objects - 2) * obj_size + num_pages * actual_page_alloc_size;
	const idx_t total_memory_limit = initial_memory + after_eviction_memory;
	buffer_pool.SetLimit(total_memory_limit, EXCEPTION_POSTSCRIPT);

	// Add object cache entries first
	for (idx_t idx = 0; idx < num_non_evictable_objects; ++idx) {
		cache.Put(StringUtil::Format("non-evictable-obj%llu", idx), make_shared_ptr<NonEvictableObject>(idx));
	}
	for (idx_t idx = 0; idx < num_evictable_objects; ++idx) {
		cache.Put(StringUtil::Format("evictable-obj%llu", idx), make_shared_ptr<EvictableTestObject>(idx, obj_size));
	}
	REQUIRE(cache.GetEntryCount() == num_objects);

	// Now pin many pages, which makes sure the eviction of object cache entries
	vector<BufferHandle> pinned_pages;
	pinned_pages.reserve(num_pages);
	for (idx_t idx = 0; idx < num_pages; ++idx) {
		// If allocation exceeds memory limit, object cache entries will be evicted first.
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, page_size, /*can_destroy=*/true);
		pinned_pages.emplace_back(std::move(pin));
	}

	// Check evictable object cache entries are partially evicted.
	vector<idx_t> evicted_entries;
	for (idx_t idx = 0; idx < num_evictable_objects; ++idx) {
		auto obj = cache.GetObject(StringUtil::Format("evictable-obj%llu", idx));
		if (obj == nullptr) {
			evicted_entries.emplace_back(idx);
		}
	}
	// Check some of the evictable cache entries have been evicted, and eviction is performed in the order of insertion.
	REQUIRE(evicted_entries == vector<idx_t> {0, 1});

	// Check non-evictable object cache entries are still there.
	for (idx_t idx = 0; idx < num_non_evictable_objects; ++idx) {
		auto obj = cache.GetObject(StringUtil::Format("non-evictable-obj%llu", idx));
		REQUIRE(obj != nullptr);
	}

	// Check overall memory usage is equal to memory limit.
	const auto final_memory_usage = buffer_manager.GetUsedMemory();
	REQUIRE(final_memory_usage == total_memory_limit);
}

TEST_CASE("Test buffer pool eviction: failed to allocate space if every page and object cache entries non-evictable",
          "[storage][buffer_pool]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();
	auto &cache = ObjectCache::GetObjectCache(context);
	const idx_t initial_memory = buffer_pool.GetUsedMemory();

	// Set a memory limit that will force eviction
	constexpr idx_t page_size = 1024 * 1024; // 1MiB per page
	constexpr idx_t num_non_evictable_objects = 5;
	constexpr idx_t num_pages = 6;
	const idx_t actual_page_alloc_size = BufferManager::GetAllocSize(page_size + Storage::DEFAULT_BLOCK_HEADER_SIZE);

	// Set limit to hold all pages
	const idx_t total_memory_limit = initial_memory + num_pages * actual_page_alloc_size;
	buffer_pool.SetLimit(total_memory_limit, EXCEPTION_POSTSCRIPT);

	// Add object cache entries first
	for (idx_t idx = 0; idx < num_non_evictable_objects; ++idx) {
		cache.Put(StringUtil::Format("non-evictable-obj%llu", idx), make_shared_ptr<NonEvictableObject>(idx));
	}
	const idx_t after_objects_memory = buffer_pool.GetUsedMemory();
	REQUIRE(after_objects_memory == initial_memory);
	REQUIRE(cache.GetEntryCount() == num_non_evictable_objects);

	// Now pin many pages, which makes sure the eviction of object cache entries
	vector<BufferHandle> pinned_pages;
	pinned_pages.reserve(num_pages);
	for (idx_t idx = 0; idx < num_pages; ++idx) {
		// If allocation exceeds memory limit, object cache entries will be evicted first.
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, page_size, /*can_destroy=*/true);
		pinned_pages.emplace_back(std::move(pin));
	}

	// If we allocate one more page, it will fail.
	REQUIRE_THROWS(buffer_manager.Allocate(MemoryTag::EXTENSION, page_size, /*can_destroy=*/true));

	// Check non-evictable entries are still there untouched, and overall memory usage is equal to memory limit.
	for (idx_t idx = 0; idx < num_non_evictable_objects; ++idx) {
		auto obj = cache.GetObject(StringUtil::Format("non-evictable-obj%llu", idx));
		REQUIRE(obj != nullptr);
	}
	const auto final_memory_usage = buffer_manager.GetUsedMemory();
	REQUIRE(final_memory_usage == total_memory_limit);
}

namespace {

idx_t SumDeadNodes(const vector<EvictionQueueInformation> &info) {
	idx_t total = 0;
	for (const auto &q : info) {
		total += q.dead_nodes;
	}
	return total;
}

idx_t SumApproxSize(const vector<EvictionQueueInformation> &info) {
	idx_t total = 0;
	for (const auto &q : info) {
		total += q.approximate_size;
	}
	return total;
}

} // namespace

// Regression test for an eviction-queue dead-node accounting bug.
//
// Each non-tiny BlockMemory placed in the eviction queue stays referenced from there via a
// weak_ptr<BlockMemory> + sequence number. When the BlockMemory is destroyed, the latest queue
// entry pointing at it becomes dead and must be reflected in the per-queue dead_nodes counter.
//
// Previously, BlockMemory::~BlockMemory only called IncrementDeadNodes when GetBuffer() was
// non-null. But if the block had already been evicted (buffer destroyed) before the BlockHandle
// was dropped, the counter was never incremented for it, even though a stale entry still sat in
// the queue. This systematically under-counted dead_nodes and made the purge-ratio early-out
// fire too aggressively, letting dead entries accumulate.
TEST_CASE("Test eviction queue: dead_nodes is incremented on BlockMemory destruction even after eviction",
          "[storage][buffer_pool]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();
	const idx_t initial_memory = buffer_pool.GetUsedMemory();

	constexpr idx_t buffer_size = 1024 * 1024; // 1 MiB
	constexpr idx_t total_buffers = 6;
	constexpr idx_t held_buffers = 2;
	const idx_t actual_alloc_size = BufferManager::GetAllocSize(buffer_size + Storage::DEFAULT_BLOCK_HEADER_SIZE);

	// Memory limit only large enough to hold `held_buffers` blocks at once, so subsequent
	// allocations evict older destroyable blocks (their buffer becomes nullptr).
	const idx_t memory_limit = initial_memory + held_buffers * actual_alloc_size;
	buffer_pool.SetLimit(memory_limit, EXCEPTION_POSTSCRIPT);

	vector<shared_ptr<BlockHandle>> handles;
	handles.reserve(total_buffers);
	for (idx_t i = 0; i < total_buffers; ++i) {
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, buffer_size, /*can_destroy=*/true);
		handles.emplace_back(pin.GetBlockHandle());
		// Pin destroyed at scope exit -> block enters the eviction queue.
	}

	// At this point only the most recent `held_buffers` BlockHandles still own a buffer; the
	// older `total_buffers - held_buffers` have been evicted (buffer == nullptr) but the
	// BlockMemory is still alive because we hold the shared_ptr<BlockHandle>.
	idx_t evicted_observed = 0;
	idx_t loaded_observed = 0;
	for (auto &handle : handles) {
		if (handle->GetMemory().GetState() == BlockState::BLOCK_UNLOADED) {
			evicted_observed++;
		} else {
			loaded_observed++;
		}
	}
	REQUIRE(evicted_observed == total_buffers - held_buffers);
	REQUIRE(loaded_observed == held_buffers);

	const idx_t dead_before = SumDeadNodes(buffer_pool.GetEvictionQueueInfo());

	// Drop all BlockHandles. Every ~BlockMemory must increment dead_nodes once, regardless
	// of whether the buffer was already null at destruction time.
	handles.clear();

	const idx_t dead_after = SumDeadNodes(buffer_pool.GetEvictionQueueInfo());

	REQUIRE(dead_after - dead_before == total_buffers);
}

// Sanity check the dead_nodes counter never exceeds the queue size and never decrements past
// zero across a destroyable-block churn workload. This indirectly exercises PurgeIteration:
// before the fix, pinned-but-current entries dequeued during purge would be both dropped from
// the queue AND wrongly subtracted from the dead-node counter, leading to underflow on the
// unsigned counter (observable as an absurdly large dead_nodes value).
TEST_CASE("Test eviction queue: dead_nodes invariants hold under destroyable block churn", "[storage][buffer_pool]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();
	const idx_t initial_memory = buffer_pool.GetUsedMemory();

	constexpr idx_t buffer_size = 64 * 1024; // 64 KiB - smaller buffers so we can churn many of them
	constexpr idx_t resident_buffers = 32;
	constexpr idx_t churn_iterations = 20000; // > INSERT_INTERVAL (4096) to trigger purges
	const idx_t actual_alloc_size = BufferManager::GetAllocSize(buffer_size + Storage::DEFAULT_BLOCK_HEADER_SIZE);

	const idx_t memory_limit = initial_memory + resident_buffers * actual_alloc_size;
	buffer_pool.SetLimit(memory_limit, EXCEPTION_POSTSCRIPT);

	// Keep a small set of pinned buffers that stay resident throughout the test.
	// Their queue entries (after each unpin/repin cycle below) are alive, latest-version,
	// and currently unevictable - exactly the case the old PurgeIteration mishandled.
	vector<BufferHandle> pinned_resident;
	pinned_resident.reserve(resident_buffers / 4);
	for (idx_t i = 0; i < resident_buffers / 4; ++i) {
		pinned_resident.emplace_back(buffer_manager.Allocate(MemoryTag::EXTENSION, buffer_size, /*can_destroy=*/true));
	}

	// Drive churn: allocate, briefly hold, release. Each unpin enqueues a node; many of
	// them go stale immediately when the BlockHandle is dropped, generating dead nodes.
	for (idx_t i = 0; i < churn_iterations; ++i) {
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, buffer_size, /*can_destroy=*/true);
		// pin destroyed -> enqueue; BlockHandle dropped -> BlockMemory destroyed -> dead++
	}

	const auto info = buffer_pool.GetEvictionQueueInfo();
	const idx_t dead = SumDeadNodes(info);
	const idx_t approx_size = SumApproxSize(info);

	// Underflow check: total_dead_nodes is an unsigned atomic. If the old PurgeIteration
	// over-decremented (treating pinned-but-current entries as dead), the counter would wrap
	// to a value far larger than any plausible queue size.
	REQUIRE(dead < approx_size + churn_iterations);

	// Every queue must individually satisfy dead_nodes <= total_insertions.
	for (const auto &q : info) {
		REQUIRE(q.dead_nodes <= q.total_insertions);
	}
}

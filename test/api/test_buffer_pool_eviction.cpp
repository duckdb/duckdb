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

	// Set a memory limit that will force eviction
	constexpr idx_t page_size = 1024 * 1024; // 1MiB per page
	constexpr idx_t obj_size = 1024 * 1024;  // 1MiB per object cache entry
	constexpr idx_t num_pages = 5;
	constexpr idx_t num_objects = 3;
	const idx_t actual_page_alloc_size = BufferManager::GetAllocSize(page_size + Storage::DEFAULT_BLOCK_HEADER_SIZE);

	// Headroom for the reservation mechanism.
	const idx_t headroom = 2 * actual_page_alloc_size;
	// Set limit to hold all objects and some pages, but not all pages with extra allocation
	const idx_t memory_limit = num_pages * actual_page_alloc_size + (num_objects - 2) * obj_size + headroom;
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", memory_limit)));
	const idx_t initial_memory = buffer_pool.GetUsedMemory();

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
		pages.push_back(pin.GetBlockHandle());
		// Pin is destroyed, so page is added to eviction queue
	}

	// Now try to allocate more memory, which is used to trigger unpinned pages first
	constexpr idx_t extra_memory = page_size * 2;
	auto extra_pin = buffer_manager.Allocate(MemoryTag::EXTENSION, extra_memory, /*can_destroy=*/true);
	auto extra_block = extra_pin.GetBlockHandle();

	// Verify object cache entries are still present, since pages are evicted first
	REQUIRE(cache.GetEntryCount() == num_objects);
	for (idx_t idx = 0; idx < num_objects; ++idx) {
		auto obj = cache.GetObject(StringUtil::Format("obj%llu", idx));
		REQUIRE(obj != nullptr);
	}
}

TEST_CASE("Test buffer pool eviction: pinned pages can evict object cache", "[storage][buffer_pool]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &buffer_manager = BufferManager::GetBufferManager(*con.context);
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();
	auto &cache = ObjectCache::GetObjectCache(context);

	// Set a memory limit that will force eviction
	constexpr idx_t page_size = 1024 * 1024; // 1MiB per page
	constexpr idx_t obj_size = 1024 * 1024;  // 1MiB per object cache entry
	constexpr idx_t num_objects = 5;
	constexpr idx_t num_pages = 6;
	const idx_t initial_memory = buffer_pool.GetUsedMemory();
	const idx_t actual_page_alloc_size = BufferManager::GetAllocSize(page_size + Storage::DEFAULT_BLOCK_HEADER_SIZE);

	// Set limit to hold all pages, some objects, and initial overhead
	// The limit should be tight enough to force eviction:
	// - Final state: 6 pages + 3 objects (after evicting 2 objects)
	// - Initial state: 5 objects
	// - When allocating pages, we need to evict 2 objects to make room
	// 
	// To force eviction, the limit must be:
	// - Less than: 5 objects + 6 pages
	// - But enough for: 3 objects + 6 pages
	const idx_t before_eviction_memory = num_objects * obj_size + num_pages * actual_page_alloc_size;
	const idx_t after_eviction_memory = (num_objects - 2) * obj_size + num_pages * actual_page_alloc_size;
	// Headroom for the reservation mechanism
	const idx_t headroom = actual_page_alloc_size;
	const idx_t memory_limit = after_eviction_memory + headroom;
	const idx_t total_memory_limit = initial_memory + memory_limit;
	
	// Verify the limit is set correctly
	const idx_t before_eviction_total = initial_memory + before_eviction_memory;
	REQUIRE(before_eviction_total > total_memory_limit);
	const idx_t after_eviction_total = initial_memory + after_eviction_memory;
	REQUIRE(after_eviction_total <= total_memory_limit);
	REQUIRE_NO_FAIL(con.Query(StringUtil::Format("PRAGMA memory_limit='%lldB'", total_memory_limit)));

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
		auto pin = buffer_manager.Allocate(MemoryTag::EXTENSION, page_size, /*can_destroy=*/true);
		pinned_pages.emplace_back(std::move(pin));
	}

	// Verify that some object cache entries were evicted to make room for pinned pages
	const idx_t after_pages_memory = buffer_pool.GetUsedMemory();
	REQUIRE(after_pages_memory <= total_memory_limit);

	// Verify object cache entries eviction and LRU eviction order
	constexpr idx_t expected_evicted_count = 2;
	for (idx_t idx = 0; idx < num_objects; ++idx) {
		const auto object_key = StringUtil::Format("obj%llu", idx);
		const bool expected_exists = idx >= expected_evicted_count;
		if (expected_exists) {
			REQUIRE(cache.GetObject(object_key) != nullptr);
		} else {
			REQUIRE(cache.GetObject(object_key) == nullptr);
		}
	}
	constexpr idx_t expected_remaining_objects = num_objects - 2;
	REQUIRE(cache.GetEntryCount() == expected_remaining_objects);
}

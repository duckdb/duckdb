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

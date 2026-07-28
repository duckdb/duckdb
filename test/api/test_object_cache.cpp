#include "catch.hpp"
#include "duckdb/common/lru_cache.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

namespace {

struct TestObject : public ObjectCacheEntry {
	int value;
	TestObject(int value) : value(value) {
	}
	~TestObject() override = default;
	string GetObjectType() override {
		return ObjectType();
	}
	static string ObjectType() {
		return "TestObject";
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx {};
	}
};

struct AnotherTestObject : public ObjectCacheEntry {
	int value;
	AnotherTestObject(int value) : value(value) {
	}
	~AnotherTestObject() override = default;
	string GetObjectType() override {
		return ObjectType();
	}
	static string ObjectType() {
		return "AnotherTestObject";
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx {};
	}
};

struct EvictableTestObject : public ObjectCacheEntry {
	int value;
	idx_t size;
	EvictableTestObject(int value, idx_t size) : value(value), size(size) {
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

TEST_CASE("Test ObjectCache", "[api][object_cache]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;

	auto cache = ObjectCache::Get(context);

	REQUIRE(cache.GetObject("test") == nullptr);
	cache.Put("test", make_shared_ptr<TestObject>(42));

	REQUIRE(cache.GetObject("test") != nullptr);

	cache.Delete("test");
	REQUIRE(cache.GetObject("test") == nullptr);

	REQUIRE(cache.GetOrCreate<TestObject>("test", 42) != nullptr);
	REQUIRE(cache.Get<TestObject>("test") != nullptr);
	REQUIRE(cache.GetOrCreate<TestObject>("test", 1337)->value == 42);
	REQUIRE(cache.Get<TestObject>("test")->value == 42);

	REQUIRE(cache.GetOrCreate<AnotherTestObject>("test", 13) == nullptr);
}

TEST_CASE("Database instances share isolated memory managers and object cache", "[api][object_cache][buffer_pool]") {
	auto first = make_uniq<DuckDB>();
	DBConfig second_config;
	second_config.ShareMemoryWith(*first->instance);
	DuckDB second(nullptr, &second_config);

	REQUIRE(first->instance->GetMemoryManager() == second.instance->GetMemoryManager());
	REQUIRE(&first->instance->GetBufferPool() == &second.instance->GetBufferPool());
	REQUIRE(&first->instance->GetBufferManager() != &second.instance->GetBufferManager());
	REQUIRE(&first->instance->GetObjectCache() == &second.instance->GetObjectCache());
	REQUIRE(first->instance->GetMemoryContextId() != second.instance->GetMemoryContextId());
	{
		auto &first_buffer_manager = first->instance->GetBufferManager();
		auto first_pin = first_buffer_manager.Allocate(MemoryTag::EXTENSION, 1024, true);
		auto first_block = first_pin.GetBlockHandle();
		REQUIRE(first_block->GetMemory().GetMemoryContextId() == first->instance->GetMemoryContextId());
		REQUIRE(first_block->GetMemory().GetMemoryContextId() != second.instance->GetMemoryContextId());
	}
	{
		auto &first_buffer_manager = first->instance->GetBufferManager();
		auto first_pin = first_buffer_manager.Allocate(MemoryTag::EXTENSION, 1024, true);
		auto first_queued_block = first_pin.GetBlockHandle();
		REQUIRE(!first_queued_block->GetMemory().IsUnloaded());
	}

	ObjectCache::Get(*first->instance).Put("first-only", make_shared_ptr<TestObject>(42));
	REQUIRE(ObjectCache::Get(*second.instance).Get<TestObject>("first-only") == nullptr);

	auto &shared_pool = second.instance->GetBufferPool();
	const auto initial_memory = shared_pool.GetUsedMemory();
	constexpr idx_t cache_entry_size = 1024 * 1024;
	ObjectCache::Get(*first->instance).Put("first-memory", make_shared_ptr<EvictableTestObject>(1, cache_entry_size));
	ObjectCache::Get(*second.instance).Put("second-memory", make_shared_ptr<EvictableTestObject>(2, cache_entry_size));
	REQUIRE(shared_pool.GetUsedMemory() == initial_memory + cache_entry_size * 2);

	first.reset();

	// Query with second database instance still works.
	Connection connection(second);
	auto result = connection.Query("SELECT sum(i) FROM range(10000) t(i)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(49995000));
}

TEST_CASE("Memory pressure from one database evicts another database's object cache entry",
          "[api][object_cache][buffer_pool]") {
	DuckDB first;
	DBConfig second_config;
	second_config.ShareMemoryWith(*first.instance);
	DuckDB second(nullptr, &second_config);

	auto &cache = first.instance->GetObjectCache();
	auto &buffer_pool = first.instance->GetBufferPool();
	auto &second_buffer_manager = second.instance->GetBufferManager();
	const auto first_context_id = first.instance->GetMemoryContextId();
	const auto second_context_id = second.instance->GetMemoryContextId();
	const auto initial_memory = buffer_pool.GetUsedMemory();

	constexpr idx_t page_size = 1024 * 1024;
	const auto allocation_size = BufferManager::GetAllocSize(page_size + Storage::DEFAULT_BLOCK_HEADER_SIZE);
	buffer_pool.SetLimit(initial_memory + allocation_size * 3, "");

	cache.Put(first_context_id, "first-entry", make_shared_ptr<EvictableTestObject>(1, allocation_size));
	cache.Put(second_context_id, "second-entry", make_shared_ptr<EvictableTestObject>(2, allocation_size));
	REQUIRE(cache.GetEntryCount() == 2);

	vector<BufferHandle> second_pins;
	second_pins.emplace_back(second_buffer_manager.Allocate(MemoryTag::EXTENSION, page_size, true));
	second_pins.emplace_back(second_buffer_manager.Allocate(MemoryTag::EXTENSION, page_size, true));

	REQUIRE(cache.Get<EvictableTestObject>(first_context_id, "first-entry") == nullptr);
	REQUIRE(cache.Get<EvictableTestObject>(second_context_id, "second-entry") != nullptr);
	REQUIRE(buffer_pool.GetUsedMemory() == initial_memory + allocation_size * 3);
}

TEST_CASE("ObjectCache drops non-evictable entries for a memory context", "[api][object_cache][buffer_pool]") {
	auto first = make_uniq<DuckDB>();
	DBConfig second_config;
	second_config.ShareMemoryWith(*first->instance);
	DuckDB second(nullptr, &second_config);

	auto &cache = first->instance->GetObjectCache();
	auto first_context_id = first->instance->GetMemoryContextId();
	auto second_context_id = second.instance->GetMemoryContextId();
	constexpr idx_t obj_size = 1024 * 1024;

	REQUIRE(cache.IsEmpty());
	cache.Put(first_context_id, "first-non-evictable", make_shared_ptr<TestObject>(1));
	cache.Put(second_context_id, "second-non-evictable", make_shared_ptr<TestObject>(2));
	cache.Put(first_context_id, "first-evictable", make_shared_ptr<EvictableTestObject>(3, obj_size));

	REQUIRE(cache.GetCurrentMemory() == obj_size);
	REQUIRE(cache.GetEntryCount() == 3);

	first.reset();

	REQUIRE(cache.Get<TestObject>(first_context_id, "first-non-evictable") == nullptr);
	REQUIRE(cache.Get<TestObject>(second_context_id, "second-non-evictable") != nullptr);
	REQUIRE(cache.Get<EvictableTestObject>(first_context_id, "first-evictable") != nullptr);
	REQUIRE(cache.GetCurrentMemory() == obj_size);
	REQUIRE(cache.GetEntryCount() == 2);
	REQUIRE(!cache.IsEmpty());

	cache.DropNonEvictableEntries(second_context_id);
	REQUIRE(cache.GetCurrentMemory() == obj_size);
	REQUIRE(cache.GetEntryCount() == 1);

	cache.Delete(first_context_id, "first-evictable");
	REQUIRE(cache.GetCurrentMemory() == 0);
	REQUIRE(cache.IsEmpty());
}

TEST_CASE("Test ObjectCache memory accounting", "[api][object_cache]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto cache = ObjectCache::Get(context);
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();
	const idx_t initial_memory = buffer_pool.GetUsedMemory();

	// Put and check accountable memory for buffer pool.
	constexpr idx_t obj_size = 1024 * 1024;
	cache.Put("evictable1", make_shared_ptr<EvictableTestObject>(1, obj_size));
	const idx_t after_put_memory = buffer_pool.GetUsedMemory();
	REQUIRE(after_put_memory == initial_memory + obj_size);

	// Delete and check accountable memory for buffer pool.
	cache.Delete("evictable1");
	const idx_t after_delete_memory = buffer_pool.GetUsedMemory();
	REQUIRE(after_delete_memory == initial_memory);
}

TEST_CASE("Test ObjectCache Manual Eviction", "[api][object_cache]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto cache = ObjectCache::Get(context);
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();
	const idx_t initial_memory = buffer_pool.GetUsedMemory();
	REQUIRE(cache.IsEmpty());

	// Put and check accountable memory for buffer pool.
	constexpr idx_t obj_size = 1024 * 1024;
	constexpr idx_t obj_count = 10;
	for (idx_t idx = 0; idx < obj_count; ++idx) {
		cache.Put(StringUtil::Format("evictable%llu", idx), make_shared_ptr<EvictableTestObject>(idx, obj_size));
	}
	REQUIRE(cache.GetEntryCount() == 10);
	const idx_t after_put_memory = buffer_pool.GetUsedMemory();
	REQUIRE(after_put_memory == initial_memory + obj_size * obj_count);

	// Evict 5 objects, leaving 5 objects in cache
	const idx_t bytes_to_free = 5 * obj_size;
	idx_t freed = cache.EvictToReduceMemory(bytes_to_free);
	REQUIRE(freed >= bytes_to_free); // Should free at least the requested amount
	REQUIRE(cache.GetCurrentMemory() == 5 * obj_size);
	REQUIRE(cache.GetEntryCount() == 5);

	// First five items should be evicted.
	for (idx_t idx = 0; idx < 5; ++idx) {
		auto value = cache.GetObject(StringUtil::Format("evictable%llu", idx));
		REQUIRE(value == nullptr);
	}

	// Later five items should be kept.
	for (idx_t idx = 5; idx < 10; ++idx) {
		auto value = cache.GetObject(StringUtil::Format("evictable%llu", idx));
		REQUIRE(value != nullptr);
	}
	REQUIRE(!cache.IsEmpty());
}

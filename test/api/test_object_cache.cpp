#include "catch.hpp"
#include "duckdb/common/lru_cache.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
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

	auto &cache = ObjectCache::GetObjectCache(context);
	auto memory_context_id = DatabaseInstance::GetDatabase(context).GetMemoryContextId();

	REQUIRE(cache.GetObject(memory_context_id, "test") == nullptr);
	cache.Put(memory_context_id, "test", make_shared_ptr<TestObject>(42));

	REQUIRE(cache.GetObject(memory_context_id, "test") != nullptr);

	cache.Delete(memory_context_id, "test");
	REQUIRE(cache.GetObject(memory_context_id, "test") == nullptr);

	REQUIRE(cache.GetOrCreate<TestObject>(memory_context_id, "test", 42) != nullptr);
	REQUIRE(cache.Get<TestObject>(memory_context_id, "test") != nullptr);
	REQUIRE(cache.GetOrCreate<TestObject>(memory_context_id, "test", 1337)->value == 42);
	REQUIRE(cache.Get<TestObject>(memory_context_id, "test")->value == 42);

	REQUIRE(cache.GetOrCreate<AnotherTestObject>(memory_context_id, "test", 13) == nullptr);
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

	first->instance->GetObjectCache().Put(first->instance->GetMemoryContextId(), "first-only",
	                                      make_shared_ptr<TestObject>(42));
	REQUIRE(second.instance->GetObjectCache().Get<TestObject>(second.instance->GetMemoryContextId(), "first-only") ==
	        nullptr);

	auto &shared_pool = second.instance->GetBufferPool();
	const auto initial_memory = shared_pool.GetUsedMemory();
	constexpr idx_t cache_entry_size = 1024 * 1024;
	first->instance->GetObjectCache().Put(first->instance->GetMemoryContextId(), "first-memory",
	                                      make_shared_ptr<EvictableTestObject>(1, cache_entry_size));
	second.instance->GetObjectCache().Put(second.instance->GetMemoryContextId(), "second-memory",
	                                      make_shared_ptr<EvictableTestObject>(2, cache_entry_size));
	REQUIRE(shared_pool.GetUsedMemory() == initial_memory + cache_entry_size * 2);

	first.reset();

	// Query with second database instance still works.
	Connection connection(second);
	auto result = connection.Query("SELECT sum(i) FROM range(10000) t(i)");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->GetValue(0, 0) == Value::BIGINT(49995000));
}

TEST_CASE("Test ObjectCache memory accounting", "[api][object_cache]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &cache = ObjectCache::GetObjectCache(context);
	auto memory_context_id = DatabaseInstance::GetDatabase(context).GetMemoryContextId();
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();
	const idx_t initial_memory = buffer_pool.GetUsedMemory();

	// Put and check accountable memory for buffer pool.
	constexpr idx_t obj_size = 1024 * 1024;
	cache.Put(memory_context_id, "evictable1", make_shared_ptr<EvictableTestObject>(1, obj_size));
	const idx_t after_put_memory = buffer_pool.GetUsedMemory();
	REQUIRE(after_put_memory == initial_memory + obj_size);

	// Delete and check accountable memory for buffer pool.
	cache.Delete(memory_context_id, "evictable1");
	const idx_t after_delete_memory = buffer_pool.GetUsedMemory();
	REQUIRE(after_delete_memory == initial_memory);
}

TEST_CASE("Test ObjectCache Manual Eviction", "[api][object_cache]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &cache = ObjectCache::GetObjectCache(context);
	auto memory_context_id = DatabaseInstance::GetDatabase(context).GetMemoryContextId();
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();
	const idx_t initial_memory = buffer_pool.GetUsedMemory();
	REQUIRE(cache.IsEmpty());

	// Put and check accountable memory for buffer pool.
	constexpr idx_t obj_size = 1024 * 1024;
	constexpr idx_t obj_count = 10;
	for (idx_t idx = 0; idx < obj_count; ++idx) {
		cache.Put(memory_context_id, StringUtil::Format("evictable%llu", idx),
		          make_shared_ptr<EvictableTestObject>(idx, obj_size));
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
		auto value = cache.GetObject(memory_context_id, StringUtil::Format("evictable%llu", idx));
		REQUIRE(value == nullptr);
	}

	// Later five items should be kept.
	for (idx_t idx = 5; idx < 10; ++idx) {
		auto value = cache.GetObject(memory_context_id, StringUtil::Format("evictable%llu", idx));
		REQUIRE(value != nullptr);
	}
	REQUIRE(!cache.IsEmpty());
}

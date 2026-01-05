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

TEST_CASE("Test ObjectCache memory accounting", "[api][object_cache]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;
	auto &cache = ObjectCache::GetObjectCache(context);
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
	auto &cache = ObjectCache::GetObjectCache(context);
	auto &buffer_pool = DatabaseInstance::GetDatabase(context).GetBufferPool();
	const idx_t initial_memory = buffer_pool.GetUsedMemory();

	// Put and check accountable memory for buffer pool.
	constexpr idx_t obj_size = 1024 * 1024;
	constexpr idx_t obj_count = 10;
	for (idx_t i = 0; i < obj_count; i++) {
		cache.Put(StringUtil::Format("evictable%llu", i), make_shared_ptr<EvictableTestObject>(i, obj_size));
	}
	REQUIRE(cache.GetEntryCount() == 10);
	const idx_t after_put_memory = buffer_pool.GetUsedMemory();
	REQUIRE(after_put_memory == initial_memory + obj_size * obj_count);

	// Evict until requested memory.
	idx_t target_memory = 5 * 1024 * 1024;
	idx_t freed = cache.EvictToReduceMemory(target_memory);
	REQUIRE(cache.GetCurrentMemory() == target_memory);
	REQUIRE(cache.GetEntryCount() == 5);
	REQUIRE(freed == obj_size * obj_count - target_memory);

	// First five items should be evicted.
	for (idx_t i = 0; i < 5; i++) {
		auto value = cache.GetObject(StringUtil::Format("evictable%llu", i));
		REQUIRE(value == nullptr);
	}

	// Later five items should be kept.
	for (idx_t i = 5; i < 10; i++) {
		auto value = cache.GetObject(StringUtil::Format("evictable%llu", i));
		REQUIRE(value != nullptr);
	}
}

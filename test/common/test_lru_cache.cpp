#include "duckdb/common/lru_cache.hpp"
#include "catch.hpp"

using namespace duckdb;

namespace {

// Test value type
struct TestValue {
	int value = 0;
	idx_t size = 0;

	TestValue(int val, idx_t sz = 100) : value(val), size(sz) {
	}
	idx_t GetRoughCacheMemory() const {
		return size;
	}
};

} // namespace

TEST_CASE("LRU Cache Basic Operations", "[lru_cache]") {
	SharedLruCache<string, TestValue> cache(1000);

	SECTION("Put and Get") {
		auto val1 = make_shared_ptr<TestValue>(42, 100);
		cache.Put("key1", val1);

		auto result = cache.Get("key1");
		REQUIRE(result != nullptr);
		REQUIRE(result->value == 42);
		REQUIRE(cache.Size() == 1);
		REQUIRE(cache.CurrentMemory() == 100);
	}

	SECTION("Get non-existent key") {
		auto result = cache.Get("nonexistent");
		REQUIRE(result == nullptr);
	}

	SECTION("Replace existing key") {
		auto val1 = make_shared_ptr<TestValue>(1, 100);
		auto val2 = make_shared_ptr<TestValue>(2, 150);

		cache.Put("key1", val1);
		cache.Put("key1", val2);

		auto result = cache.Get("key1");
		REQUIRE(result != nullptr);
		REQUIRE(result->value == 2);
		REQUIRE(cache.CurrentMemory() == 150);
	}

	SECTION("Delete") {
		auto val1 = make_shared_ptr<TestValue>(42, 100);
		cache.Put("key1", val1);

		bool deleted = cache.Delete("key1");
		REQUIRE(deleted == true);
		REQUIRE(cache.Get("key1") == nullptr);
		REQUIRE(cache.Size() == 0);
		REQUIRE(cache.CurrentMemory() == 0);

		const bool deleted_again = cache.Delete("key1");
		REQUIRE(!deleted_again);
	}
}

TEST_CASE("LRU Cache Eviction", "[lru_cache]") {
	SharedLruCache<string, TestValue> cache(500);

	SECTION("Evict LRU when exceeding max weight") {
		auto val1 = make_shared_ptr<TestValue>(1, 200);
		auto val2 = make_shared_ptr<TestValue>(2, 200);
		auto val3 = make_shared_ptr<TestValue>(3, 200);

		cache.Put("key1", val1);
		cache.Put("key2", val2);
		cache.Put("key3", val3);

		// Should evict key1 (LRU) to make room
		REQUIRE(cache.Get("key1") == nullptr);
		REQUIRE(cache.Get("key2") != nullptr);
		REQUIRE(cache.Get("key3") != nullptr);
		REQUIRE(cache.Size() == 2);
		REQUIRE(cache.CurrentMemory() <= 500);
	}

	SECTION("LRU ordering") {
		auto val1 = make_shared_ptr<TestValue>(1, 100);
		auto val2 = make_shared_ptr<TestValue>(2, 100);
		auto val3 = make_shared_ptr<TestValue>(3, 100);
		auto val4 = make_shared_ptr<TestValue>(4, 100);

		cache.Put("key1", val1);
		cache.Put("key2", val2);
		cache.Put("key3", val3);

		// Access key1 to make it MRU
		cache.Get("key1");

		// Add key4 - should evict key2 (LRU, not key1)
		cache.Put("key4", val4);

		REQUIRE(cache.Get("key1") != nullptr);
		REQUIRE(cache.Get("key2") == nullptr);
		REQUIRE(cache.Get("key3") != nullptr);
		REQUIRE(cache.Get("key4") != nullptr);
	}
}

TEST_CASE("LRU Cache Unlimited Memory", "[lru_cache]") {
	SharedLruCache<string, TestValue> cache(0); // 0 = unlimited

	// Should not evict anything
	for (int idx = 0; idx < 100; ++idx) {
		auto val = make_shared_ptr<TestValue>(idx, 100);
		cache.Put("key" + std::to_string(idx), val);
	}

	REQUIRE(cache.Size() == 100);
}

TEST_CASE("LRU Cache Clear", "[lru_cache]") {
	SharedLruCache<string, TestValue> cache(1000);

	auto val1 = make_shared_ptr<TestValue>(1, 100);
	auto val2 = make_shared_ptr<TestValue>(2, 100);
	cache.Put("key1", val1);
	cache.Put("key2", val2);

	cache.Clear();

	REQUIRE(cache.Size() == 0);
	REQUIRE(cache.CurrentMemory() == 0);
	REQUIRE(cache.Get("key1") == nullptr);
	REQUIRE(cache.Get("key2") == nullptr);
}

TEST_CASE("LRU Cache Put with Explicit Size", "[lru_cache]") {
	SharedLruCache<string, TestValue> cache(1000);

	SECTION("Put with explicit size") {
		auto val1 = make_shared_ptr<TestValue>(42, 100);
		// Use explicit size API - override the value's GetRoughCacheMemory()
		cache.Put("key1", val1, 200);

		auto result = cache.Get("key1");
		REQUIRE(result != nullptr);
		REQUIRE(result->value == 42);
		REQUIRE(cache.CurrentMemory() == 200); // Uses explicit size, not value->GetRoughCacheMemory()
	}

	SECTION("Put with GetRoughCacheMemory vs explicit size") {
		auto val1 = make_shared_ptr<TestValue>(42, 100);
		// First use GetRoughCacheMemory API
		cache.Put("key1", val1);
		REQUIRE(cache.CurrentMemory() == 100);

		// Then replace with explicit size
		cache.Put("key1", val1, 300);
		REQUIRE(cache.CurrentMemory() == 300);
	}
}

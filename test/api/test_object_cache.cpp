#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/storage/object_cache.hpp"

using namespace duckdb;
using namespace std;

struct TestObject : public ObjectCacheEntry {
	int value;

	TestObject(int value) : value(value) {
	}

	string GetObjectType() override {
		return ObjectType();
	}

	static string ObjectType() {
		return "TestObject";
	}
};

struct AnotherTestObject : public ObjectCacheEntry {
	int value;
	AnotherTestObject(int value) : value(value) {
	}
	string GetObjectType() override {
		return ObjectType();
	}

	static string ObjectType() {
		return "AnotherTestObject";
	}
};

TEST_CASE("Test ObjectCache", "[api]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;

	auto &cache = ObjectCache::GetObjectCache(context);

	REQUIRE(cache.GetObject("test") == nullptr);
	cache.Put("test", make_shared<TestObject>(42));

	REQUIRE(cache.GetObject("test") != nullptr);

	cache.Delete("test");
	REQUIRE(cache.GetObject("test") == nullptr);

	REQUIRE(cache.GetOrCreate<TestObject>("test", 42) != nullptr);
	REQUIRE(cache.Get<TestObject>("test") != nullptr);
	REQUIRE(cache.GetOrCreate<TestObject>("test", 1337)->value == 42);
	REQUIRE(cache.Get<TestObject>("test")->value == 42);

	REQUIRE(cache.GetOrCreate<AnotherTestObject>("test", 13) == nullptr);
}

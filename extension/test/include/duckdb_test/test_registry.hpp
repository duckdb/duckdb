#pragma once

#include "duckdb_test/test_bundle.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb_test {

class TestRegistryEntry : public duckdb::ObjectCacheEntry {
public:
	TestRegistryEntry(TestRegistry registry_p, void (*destroy_p)(void *)) : registry(registry_p), destroy(destroy_p) {
	}
	~TestRegistryEntry() override {
		destroy(registry.data);
	}
	static duckdb::string ObjectType() {
		return "duckdb.test.registry";
	}
	duckdb::string GetObjectType() override {
		return ObjectType();
	}
	duckdb::optional_idx GetEstimatedCacheMemory() const override {
		return duckdb::optional_idx();
	}

	TestRegistry registry;

private:
	void (*destroy)(void *);
};

} // namespace duckdb_test

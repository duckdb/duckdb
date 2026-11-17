#include "test_capi_v2.hpp"

#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// V2 object cache: the per-instance cache of pinned and evictable entries.
//
// Callbacks avoid Catch assertions: they run inside DuckDB. They record what
// they see in their user data, and the test asserts on it afterwards.
// ---------------------------------------------------------------------------

namespace test_capi_v2 {

namespace {

// Reports a failure through the live error slot a callback receives.
void Fail(duckdb_v2_error_info_handle *err, const char *message) {
	duckdb_v2_error_info_set_code(*err, DUCKDB_V2_ERROR_INPUT_INVALID);
	duckdb_v2_error_info_set_text(*err, message, std::strlen(message));
}

struct CacheData {
	std::string value;
	bool *destroyed = nullptr;
};

void DestroyCacheData(void *ptr) {
	auto data = static_cast<CacheData *>(ptr);
	if (data->destroyed) {
		*data->destroyed = true;
	}
	delete data;
}

struct Creator {
	idx_t creations = 0;
	idx_t size_bytes = 0;
	bool fail = false;
	bool *destroyed = nullptr;
};

void CreateCacheData(void *user_data, duckdb_v2_opaque *data, idx_t *size_bytes, duckdb_v2_error_info_handle *err) {
	auto &creator = *static_cast<Creator *>(user_data);
	creator.creations++;
	auto created = new CacheData {"created", creator.destroyed};
	*data = {created, DestroyCacheData, nullptr};
	*size_bytes = creator.size_bytes;
	if (creator.fail) {
		Fail(err, "creation refused");
	}
}

std::string ValueOf(duckdb_v2_object_cache_entry_handle entry) {
	void *data = nullptr;
	REQUIRE(duckdb_v2_object_cache_entry_get_data(entry, &data, nullptr) == DUCKDB_V2_ERROR_NONE);
	return static_cast<CacheData *>(data)->value;
}

} // namespace

TEST_CASE("V2 object cache: put, get, delete", "[capi_v2][state][object_cache]") {
	EnvFixture fx;
	duckdb_v2_object_cache_handle cache = nullptr;
	REQUIRE(duckdb_v2_instance_get_object_cache(fx.instance, &cache, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(cache != nullptr);

	const char *type = "v2_test.footer";
	const char *key = "file.parquet";

	duckdb_v2_object_cache_entry_handle entry = nullptr;
	REQUIRE(duckdb_v2_object_cache_get(cache, type, std::strlen(type), key, std::strlen(key), &entry, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(entry == nullptr);

	bool destroyed = false;
	duckdb_v2_opaque data = {new CacheData {"first", &destroyed}, DestroyCacheData, nullptr};
	// evictable
	REQUIRE(duckdb_v2_object_cache_put(cache, type, std::strlen(type), key, std::strlen(key), &data, 64, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_object_cache_get(cache, type, std::strlen(type), key, std::strlen(key), &entry, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(entry != nullptr);
	REQUIRE(ValueOf(entry) == "first");

	// the same key under another type is another entry
	const char *other_type = "v2_test.other";
	duckdb_v2_object_cache_entry_handle other = nullptr;
	REQUIRE(duckdb_v2_object_cache_get(cache, other_type, std::strlen(other_type), key, std::strlen(key), &other,
	                                   nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(other == nullptr);

	// a replaced entry lives on while it is held
	duckdb_v2_opaque replacement = {new CacheData {"second"}, DestroyCacheData, nullptr};
	REQUIRE(duckdb_v2_object_cache_put(cache, type, std::strlen(type), key, std::strlen(key), &replacement, 0,
	                                   nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(destroyed);
	REQUIRE(ValueOf(entry) == "first");
	REQUIRE(duckdb_v2_object_cache_entry_destroy(&entry) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(entry == nullptr);
	REQUIRE(destroyed);

	REQUIRE(duckdb_v2_object_cache_get(cache, type, std::strlen(type), key, std::strlen(key), &entry, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(ValueOf(entry) == "second");
	duckdb_v2_object_cache_entry_destroy(&entry);

	REQUIRE(duckdb_v2_object_cache_delete(cache, type, std::strlen(type), key, std::strlen(key), nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_object_cache_get(cache, type, std::strlen(type), key, std::strlen(key), &entry, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(entry == nullptr);
	// deleting what is not there is fine
	REQUIRE(duckdb_v2_object_cache_delete(cache, type, std::strlen(type), key, std::strlen(key), nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 object cache: get or create", "[capi_v2][state][object_cache]") {
	EnvFixture fx;
	duckdb_v2_object_cache_handle cache = nullptr;
	REQUIRE(duckdb_v2_instance_get_object_cache(fx.instance, &cache, nullptr) == DUCKDB_V2_ERROR_NONE);
	const char *type = "v2_test.created";
	const char *key = "k";

	Creator creator;
	duckdb_v2_object_cache_entry_handle first = nullptr;
	duckdb_v2_object_cache_entry_handle second = nullptr;
	REQUIRE(duckdb_v2_object_cache_get_or_create(cache, type, std::strlen(type), key, std::strlen(key), CreateCacheData,
	                                             &creator, &first, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_object_cache_get_or_create(cache, type, std::strlen(type), key, std::strlen(key), CreateCacheData,
	                                             &creator, &second, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(creator.creations == 1);
	REQUIRE(ValueOf(first) == "created");
	void *first_data = nullptr;
	void *second_data = nullptr;
	duckdb_v2_object_cache_entry_get_data(first, &first_data, nullptr);
	duckdb_v2_object_cache_entry_get_data(second, &second_data, nullptr);
	REQUIRE(first_data == second_data);
	duckdb_v2_object_cache_entry_destroy(&first);
	duckdb_v2_object_cache_entry_destroy(&second);

	SECTION("a failing creation caches nothing") {
		const char *failing_key = "failing";
		bool destroyed = false;
		Creator failing;
		failing.fail = true;
		failing.destroyed = &destroyed;
		duckdb_v2_object_cache_entry_handle entry = nullptr;
		REQUIRE(duckdb_v2_object_cache_get_or_create(cache, type, std::strlen(type), failing_key,
		                                             std::strlen(failing_key), CreateCacheData, &failing, &entry,
		                                             nullptr) != DUCKDB_V2_ERROR_NONE);
		REQUIRE(entry == nullptr);
		REQUIRE(destroyed);
		REQUIRE(duckdb_v2_object_cache_get(cache, type, std::strlen(type), failing_key, std::strlen(failing_key),
		                                   &entry, nullptr) == DUCKDB_V2_ERROR_NONE);
		REQUIRE(entry == nullptr);
	}
}

TEST_CASE("V2 object cache: entries die with the instance", "[capi_v2][state][object_cache]") {
	duckdb_v2_environment_handle env = nullptr;
	duckdb_v2_instance_handle instance = nullptr;
	REQUIRE(duckdb_v2_environment_create(&env, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(OpenInstance(env, std::string_view(), &instance, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_object_cache_handle cache = nullptr;
	REQUIRE(duckdb_v2_instance_get_object_cache(instance, &cache, nullptr) == DUCKDB_V2_ERROR_NONE);

	const char *type = "v2_test.lifetime";
	const char *pinned_key = "pinned";
	const char *evictable_key = "evictable";
	bool pinned_destroyed = false;
	bool evictable_destroyed = false;
	duckdb_v2_opaque pinned = {new CacheData {"pinned", &pinned_destroyed}, DestroyCacheData, nullptr};
	duckdb_v2_opaque evictable = {new CacheData {"evictable", &evictable_destroyed}, DestroyCacheData, nullptr};
	REQUIRE(duckdb_v2_object_cache_put(cache, type, std::strlen(type), pinned_key, std::strlen(pinned_key), &pinned, 0,
	                                   nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_object_cache_put(cache, type, std::strlen(type), evictable_key, std::strlen(evictable_key),
	                                   &evictable, 128, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE_FALSE(pinned_destroyed);
	REQUIRE_FALSE(evictable_destroyed);

	duckdb_v2_instance_destroy(&instance);
	REQUIRE(pinned_destroyed);
	REQUIRE(evictable_destroyed);
	duckdb_v2_environment_destroy(&env);
}

} // namespace test_capi_v2

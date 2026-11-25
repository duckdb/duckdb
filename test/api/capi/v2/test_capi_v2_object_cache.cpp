#include "test_capi_v2.hpp"

#include <atomic>
#include <chrono>
#include <limits>
#include <string>
#include <thread>
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
	std::atomic<idx_t> creations {0};
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

//! A creator that holds its creation until `*wait_for` reaches `wait_count`, giving up after a while.
struct SlowCreator : Creator {
	std::atomic<idx_t> arrived {0};
	std::atomic<idx_t> *wait_for = nullptr;
	idx_t wait_count = 0;
	std::atomic<bool> timed_out {false};
};

void CreateSlowly(void *user_data, duckdb_v2_opaque *data, idx_t *size_bytes, duckdb_v2_error_info_handle *err) {
	auto &creator = *static_cast<SlowCreator *>(user_data);
	creator.creations++;
	if (creator.wait_for) {
		const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
		while (*creator.wait_for < creator.wait_count) {
			if (std::chrono::steady_clock::now() > deadline) {
				creator.timed_out = true;
				break;
			}
			std::this_thread::yield();
		}
		// let the calls that just arrived reach the wait
		std::this_thread::sleep_for(std::chrono::milliseconds(50));
	}
	*data = {new CacheData {"slow", creator.destroyed}, DestroyCacheData, nullptr};
	*size_bytes = creator.size_bytes;
	if (creator.fail) {
		Fail(err, "creation refused");
	}
}

//! The memory the instance accounts to the object cache, in bytes.
idx_t ObjectCacheMemory(duckdb_v2_connection_handle conn) {
	duckdb_v2_result_handle result = nullptr;
	REQUIRE(Query(conn, "SELECT memory_usage_bytes FROM duckdb_memory() WHERE tag = 'OBJECT_CACHE'", &result) ==
	        DUCKDB_V2_ERROR_NONE);
	auto chunk = StepChunk(result);
	REQUIRE(chunk != nullptr);
	duckdb_v2_vector_handle vec = nullptr;
	REQUIRE(duckdb_v2_data_chunk_get_vector(chunk, 0, &vec, nullptr) == DUCKDB_V2_ERROR_NONE);
	duckdb_v2_vector_view view {};
	REQUIRE(duckdb_v2_vector_get_view(vec, &view, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(view.count == 1);
	const auto bytes = static_cast<const int64_t *>(view.data)[SelAt(view.sel, 0)];
	duckdb_v2_data_chunk_destroy(&chunk);
	duckdb_v2_result_destroy(&result);
	return static_cast<idx_t>(bytes);
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

TEST_CASE("V2 object cache: put failures destroy the data", "[capi_v2][state][object_cache]") {
	EnvFixture fx;
	duckdb_v2_object_cache_handle cache = nullptr;
	REQUIRE(duckdb_v2_instance_get_object_cache(fx.instance, &cache, nullptr) == DUCKDB_V2_ERROR_NONE);
	const char *type = "v2_test.failures";
	const char *key = "k";

	bool destroyed = false;
	duckdb_v2_opaque data = {new CacheData {"no cache", &destroyed}, DestroyCacheData, nullptr};
	REQUIRE(duckdb_v2_object_cache_put(nullptr, type, std::strlen(type), key, std::strlen(key), &data, 0, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(destroyed);

	destroyed = false;
	data = {new CacheData {"no key", &destroyed}, DestroyCacheData, nullptr};
	REQUIRE(duckdb_v2_object_cache_put(cache, type, std::strlen(type), nullptr, 1, &data, 0, nullptr) ==
	        DUCKDB_V2_ERROR_INPUT_INVALID);
	REQUIRE(destroyed);

	// larger than the cache can hold
	destroyed = false;
	data = {new CacheData {"huge", &destroyed}, DestroyCacheData, nullptr};
	REQUIRE(duckdb_v2_object_cache_put(cache, type, std::strlen(type), key, std::strlen(key), &data,
	                                   std::numeric_limits<idx_t>::max(), nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(destroyed);
	duckdb_v2_object_cache_entry_handle entry = nullptr;
	REQUIRE(duckdb_v2_object_cache_get(cache, type, std::strlen(type), key, std::strlen(key), &entry, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(entry == nullptr);

	// a create callback reporting a size larger than the cache can hold
	destroyed = false;
	Creator huge;
	huge.size_bytes = std::numeric_limits<idx_t>::max();
	huge.destroyed = &destroyed;
	REQUIRE(duckdb_v2_object_cache_get_or_create(cache, type, std::strlen(type), key, std::strlen(key), CreateCacheData,
	                                             &huge, &entry, nullptr) != DUCKDB_V2_ERROR_NONE);
	REQUIRE(entry == nullptr);
	REQUIRE(destroyed);

	REQUIRE(duckdb_v2_object_cache_entry_destroy(nullptr) == DUCKDB_V2_ERROR_NONE);
}

TEST_CASE("V2 object cache: replacing a pinned entry with an evictable one", "[capi_v2][state][object_cache]") {
	EnvFixture fx;
	duckdb_v2_object_cache_handle cache = nullptr;
	REQUIRE(duckdb_v2_instance_get_object_cache(fx.instance, &cache, nullptr) == DUCKDB_V2_ERROR_NONE);
	const char *type = "v2_test.replace";
	const char *key = "k";

	bool pinned_destroyed = false;
	duckdb_v2_opaque pinned = {new CacheData {"pinned", &pinned_destroyed}, DestroyCacheData, nullptr};
	REQUIRE(duckdb_v2_object_cache_put(cache, type, std::strlen(type), key, std::strlen(key), &pinned, 0, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	duckdb_v2_opaque evictable = {new CacheData {"evictable"}, DestroyCacheData, nullptr};
	REQUIRE(duckdb_v2_object_cache_put(cache, type, std::strlen(type), key, std::strlen(key), &evictable, 64,
	                                   nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(pinned_destroyed);

	duckdb_v2_object_cache_entry_handle entry = nullptr;
	REQUIRE(duckdb_v2_object_cache_get(cache, type, std::strlen(type), key, std::strlen(key), &entry, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(ValueOf(entry) == "evictable");
	duckdb_v2_object_cache_entry_destroy(&entry);

	// one delete removes the entry, whichever kind it was
	REQUIRE(duckdb_v2_object_cache_delete(cache, type, std::strlen(type), key, std::strlen(key), nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_object_cache_get(cache, type, std::strlen(type), key, std::strlen(key), &entry, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(entry == nullptr);
}

TEST_CASE("V2 object cache: concurrent get or create creates once", "[capi_v2][state][object_cache]") {
	EnvFixture fx;
	duckdb_v2_object_cache_handle cache = nullptr;
	REQUIRE(duckdb_v2_instance_get_object_cache(fx.instance, &cache, nullptr) == DUCKDB_V2_ERROR_NONE);
	const char *type = "v2_test.concurrent";
	const char *key = "k";

	constexpr idx_t THREAD_COUNT = 8;
	SlowCreator creator;
	creator.size_bytes = 64;
	// the creation lasts until every thread is in, so the others have to wait for it
	creator.wait_for = &creator.arrived;
	creator.wait_count = THREAD_COUNT;
	std::atomic<idx_t> ready {0};
	std::vector<void *> seen(THREAD_COUNT, nullptr);
	std::vector<DUCKDB_V2_ERROR> results(THREAD_COUNT, DUCKDB_V2_ERROR_NONE);
	std::vector<std::thread> threads;
	for (idx_t i = 0; i < THREAD_COUNT; i++) {
		threads.emplace_back([&, i]() {
			ready++;
			while (ready < THREAD_COUNT) {
				std::this_thread::yield();
			}
			creator.arrived++;
			duckdb_v2_object_cache_entry_handle entry = nullptr;
			results[i] = duckdb_v2_object_cache_get_or_create(cache, type, std::strlen(type), key, std::strlen(key),
			                                                  CreateSlowly, &creator, &entry, nullptr);
			if (entry) {
				duckdb_v2_object_cache_entry_get_data(entry, &seen[i], nullptr);
				duckdb_v2_object_cache_entry_destroy(&entry);
			}
		});
	}
	for (auto &thread : threads) {
		thread.join();
	}
	REQUIRE(creator.creations == 1);
	REQUIRE_FALSE(creator.timed_out);
	for (idx_t i = 0; i < THREAD_COUNT; i++) {
		REQUIRE(results[i] == DUCKDB_V2_ERROR_NONE);
		REQUIRE(seen[i] != nullptr);
		REQUIRE(seen[i] == seen[0]);
	}
}

TEST_CASE("V2 object cache: a slow creation blocks no other key", "[capi_v2][state][object_cache]") {
	EnvFixture fx;
	EnvFixture other_fx;
	duckdb_v2_object_cache_handle cache = nullptr;
	duckdb_v2_object_cache_handle other_cache = nullptr;
	REQUIRE(duckdb_v2_instance_get_object_cache(fx.instance, &cache, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(duckdb_v2_instance_get_object_cache(other_fx.instance, &other_cache, nullptr) == DUCKDB_V2_ERROR_NONE);
	const char *type = "v2_test.slow";
	const char *slow_key = "slow";

	// the slow creation lasts until it is released
	std::atomic<idx_t> released {0};
	SlowCreator slow;
	slow.wait_for = &released;
	slow.wait_count = 1;
	DUCKDB_V2_ERROR slow_result = DUCKDB_V2_ERROR_INPUT_INVALID;
	std::thread slow_thread([&]() {
		duckdb_v2_object_cache_entry_handle entry = nullptr;
		slow_result = duckdb_v2_object_cache_get_or_create(cache, type, std::strlen(type), slow_key,
		                                                   std::strlen(slow_key), CreateSlowly, &slow, &entry, nullptr);
		duckdb_v2_object_cache_entry_destroy(&entry);
	});
	while (slow.creations == 0) {
		std::this_thread::yield();
	}

	// many keys, so that keys sharing any kind of lock with the slow one are among them
	constexpr idx_t KEY_COUNT = 256;
	Creator fast;
	idx_t fast_failures = 0;
	for (auto current : {cache, other_cache}) {
		for (idx_t i = 0; i < KEY_COUNT; i++) {
			auto key = "fast_" + std::to_string(i);
			duckdb_v2_object_cache_entry_handle entry = nullptr;
			if (duckdb_v2_object_cache_get_or_create(current, type, std::strlen(type), key.data(), key.size(),
			                                         CreateCacheData, &fast, &entry, nullptr) != DUCKDB_V2_ERROR_NONE) {
				fast_failures++;
			}
			duckdb_v2_object_cache_entry_destroy(&entry);
		}
	}
	// the same key on the other instance is another entry
	SlowCreator other_slow;
	duckdb_v2_object_cache_entry_handle other_entry = nullptr;
	auto other_result =
	    duckdb_v2_object_cache_get_or_create(other_cache, type, std::strlen(type), slow_key, std::strlen(slow_key),
	                                         CreateSlowly, &other_slow, &other_entry, nullptr);
	duckdb_v2_object_cache_entry_destroy(&other_entry);
	// all of the above ran while the slow creation was still going on
	const bool slow_still_running = released == 0 && !slow.timed_out;
	released++;
	slow_thread.join();

	REQUIRE(slow_still_running);
	REQUIRE(fast_failures == 0);
	REQUIRE(fast.creations == 2 * KEY_COUNT);
	REQUIRE(other_result == DUCKDB_V2_ERROR_NONE);
	REQUIRE(other_slow.creations == 1);
	REQUIRE(slow_result == DUCKDB_V2_ERROR_NONE);
	REQUIRE(slow.creations == 1);
}

TEST_CASE("V2 object cache: a failed creation lets the waiting calls retry", "[capi_v2][state][object_cache]") {
	EnvFixture fx;
	duckdb_v2_object_cache_handle cache = nullptr;
	REQUIRE(duckdb_v2_instance_get_object_cache(fx.instance, &cache, nullptr) == DUCKDB_V2_ERROR_NONE);
	const char *type = "v2_test.retry";
	const char *key = "k";

	constexpr idx_t WAITER_COUNT = 4;
	// the failing creation lasts until every waiter is about to call
	bool failed_destroyed = false;
	SlowCreator failing;
	failing.fail = true;
	failing.destroyed = &failed_destroyed;
	std::atomic<idx_t> waiting {0};
	failing.wait_for = &waiting;
	failing.wait_count = WAITER_COUNT;
	DUCKDB_V2_ERROR failing_result = DUCKDB_V2_ERROR_NONE;
	std::thread failing_thread([&]() {
		duckdb_v2_object_cache_entry_handle entry = nullptr;
		failing_result = duckdb_v2_object_cache_get_or_create(cache, type, std::strlen(type), key, std::strlen(key),
		                                                      CreateSlowly, &failing, &entry, nullptr);
		duckdb_v2_object_cache_entry_destroy(&entry);
	});
	while (failing.creations == 0) {
		std::this_thread::yield();
	}

	Creator retrying;
	std::vector<void *> seen(WAITER_COUNT, nullptr);
	std::vector<DUCKDB_V2_ERROR> results(WAITER_COUNT, DUCKDB_V2_ERROR_INPUT_INVALID);
	std::vector<std::thread> waiters;
	for (idx_t i = 0; i < WAITER_COUNT; i++) {
		waiters.emplace_back([&, i]() {
			waiting++;
			duckdb_v2_object_cache_entry_handle entry = nullptr;
			results[i] = duckdb_v2_object_cache_get_or_create(cache, type, std::strlen(type), key, std::strlen(key),
			                                                  CreateCacheData, &retrying, &entry, nullptr);
			if (entry) {
				duckdb_v2_object_cache_entry_get_data(entry, &seen[i], nullptr);
				duckdb_v2_object_cache_entry_destroy(&entry);
			}
		});
	}
	failing_thread.join();
	for (auto &waiter : waiters) {
		waiter.join();
	}

	REQUIRE_FALSE(failing.timed_out);
	REQUIRE(failing_result != DUCKDB_V2_ERROR_NONE);
	REQUIRE(failed_destroyed);
	// one waiter created the entry after the failure, and the others received it
	REQUIRE(retrying.creations == 1);
	for (idx_t i = 0; i < WAITER_COUNT; i++) {
		REQUIRE(results[i] == DUCKDB_V2_ERROR_NONE);
		REQUIRE(seen[i] != nullptr);
		REQUIRE(seen[i] == seen[0]);
	}
}

TEST_CASE("V2 object cache: only cached entries count against the memory limit", "[capi_v2][state][object_cache]") {
	EnvFixture fx;
	duckdb_v2_object_cache_handle cache = nullptr;
	REQUIRE(duckdb_v2_instance_get_object_cache(fx.instance, &cache, nullptr) == DUCKDB_V2_ERROR_NONE);
	const char *type = "v2_test.accounting";
	const char *key = "k";
	constexpr idx_t MIB = 1024 * 1024;

	const auto baseline = ObjectCacheMemory(fx.conn);
	bool first_destroyed = false;
	duckdb_v2_opaque first = {new CacheData {"first", &first_destroyed}, DestroyCacheData, nullptr};
	REQUIRE(duckdb_v2_object_cache_put(cache, type, std::strlen(type), key, std::strlen(key), &first, 4 * MIB,
	                                   nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ObjectCacheMemory(fx.conn) == baseline + 4 * MIB);
	duckdb_v2_object_cache_entry_handle first_entry = nullptr;
	REQUIRE(duckdb_v2_object_cache_get(cache, type, std::strlen(type), key, std::strlen(key), &first_entry, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	// a replaced entry that is still held is no longer counted
	bool second_destroyed = false;
	duckdb_v2_opaque second = {new CacheData {"second", &second_destroyed}, DestroyCacheData, nullptr};
	REQUIRE(duckdb_v2_object_cache_put(cache, type, std::strlen(type), key, std::strlen(key), &second, 8 * MIB,
	                                   nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ObjectCacheMemory(fx.conn) == baseline + 8 * MIB);
	REQUIRE_FALSE(first_destroyed);
	REQUIRE(ValueOf(first_entry) == "first");
	duckdb_v2_object_cache_entry_handle second_entry = nullptr;
	REQUIRE(duckdb_v2_object_cache_get(cache, type, std::strlen(type), key, std::strlen(key), &second_entry, nullptr) ==
	        DUCKDB_V2_ERROR_NONE);

	// neither is a deleted one
	REQUIRE(duckdb_v2_object_cache_delete(cache, type, std::strlen(type), key, std::strlen(key), nullptr) ==
	        DUCKDB_V2_ERROR_NONE);
	REQUIRE(ObjectCacheMemory(fx.conn) == baseline);
	REQUIRE_FALSE(second_destroyed);
	REQUIRE(ValueOf(second_entry) == "second");

	// nor an evicted one
	const char *evicted_key = "evicted";
	bool evicted_destroyed = false;
	duckdb_v2_opaque evicted = {new CacheData {"evicted", &evicted_destroyed}, DestroyCacheData, nullptr};
	REQUIRE(duckdb_v2_object_cache_put(cache, type, std::strlen(type), evicted_key, std::strlen(evicted_key), &evicted,
	                                   256 * MIB, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ObjectCacheMemory(fx.conn) == baseline + 256 * MIB);
	duckdb_v2_object_cache_entry_handle evicted_entry = nullptr;
	REQUIRE(duckdb_v2_object_cache_get(cache, type, std::strlen(type), evicted_key, std::strlen(evicted_key),
	                                   &evicted_entry, nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(RunSql(fx.conn, "SET memory_limit = '128MB'") == DUCKDB_V2_ERROR_NONE);
	REQUIRE(ObjectCacheMemory(fx.conn) == baseline);
	duckdb_v2_object_cache_entry_handle lookup = nullptr;
	REQUIRE(duckdb_v2_object_cache_get(cache, type, std::strlen(type), evicted_key, std::strlen(evicted_key), &lookup,
	                                   nullptr) == DUCKDB_V2_ERROR_NONE);
	REQUIRE(lookup == nullptr);
	REQUIRE_FALSE(evicted_destroyed);
	REQUIRE(ValueOf(evicted_entry) == "evicted");

	duckdb_v2_object_cache_entry_destroy(&first_entry);
	duckdb_v2_object_cache_entry_destroy(&second_entry);
	duckdb_v2_object_cache_entry_destroy(&evicted_entry);
	REQUIRE(first_destroyed);
	REQUIRE(second_destroyed);
	REQUIRE(evicted_destroyed);
	REQUIRE(ObjectCacheMemory(fx.conn) == baseline);
}

} // namespace test_capi_v2

#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/db_instance_cache.hpp"

#include <chrono>
#include <thread>

using namespace duckdb;
using namespace std;

static void background_thread_connect(DBInstanceCache *instance_cache, std::string *path) {
	try {
		DBConfig config;
		auto connection = instance_cache->GetOrCreateInstance(*path, config, true);
		connection.reset();
	} catch (std::exception &ex) {
		FAIL(ex.what());
	}
}

TEST_CASE("Test parallel connection and destruction of connections with database instance cache", "[api][.]") {
	DBInstanceCache instance_cache;

	for (idx_t i = 0; i < 100; i++) {
		auto path = TestCreatePath("instance_cache_parallel.db");

		DBConfig config;
		auto shared_db = instance_cache.GetOrCreateInstance(path, config, true);

		thread background_thread(background_thread_connect, &instance_cache, &path);
		shared_db.reset();
		background_thread.join();
		TestDeleteFile(path);
		REQUIRE(1);
	}
}

#include "capi_tester.hpp"
#include "duckdb.h"

#include <chrono>
#include <thread>

using namespace duckdb;
using namespace std;

static void background_thread_connect(duckdb_instance_cache instance_cache, const char *path) {
	try {
		duckdb_database out_database;
		auto state = duckdb_get_or_create_from_cache(instance_cache, path, &out_database, nullptr, nullptr);
		REQUIRE(state == DuckDBSuccess);
		duckdb_close(&out_database);
	} catch (std::exception &ex) {
		FAIL(ex.what());
	}
}

TEST_CASE("Test the database instance cache in the C API", "[api][.]") {
	auto instance_cache = duckdb_create_instance_cache();

	for (idx_t i = 0; i < 30; i++) {
		auto path = TestCreatePath("shared_db.db");

		duckdb_database shared_out_database;
		auto state =
		    duckdb_get_or_create_from_cache(instance_cache, path.c_str(), &shared_out_database, nullptr, nullptr);
		REQUIRE(state == DuckDBSuccess);

		thread background_thread(background_thread_connect, instance_cache, path.c_str());
		duckdb_close(&shared_out_database);
		background_thread.join();
		TestDeleteFile(path);
		REQUIRE(1);
	}

	duckdb_destroy_instance_cache(&instance_cache);
}

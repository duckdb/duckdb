#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/db_instance_cache.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/catalog/duck_catalog.hpp"

#include <chrono>
#include <iostream>
#include <thread>

using namespace duckdb;

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

		std::thread background_thread(background_thread_connect, &instance_cache, &path);
		shared_db.reset();
		background_thread.join();
		TestDeleteFile(path);
		REQUIRE(1);
	}
}
struct DelayingStorageExtension : StorageExtension {
	DelayingStorageExtension() {
		attach = [](StorageExtensionInfo *, ClientContext &, AttachedDatabase &db, const string &, AttachInfo &info,
		            AccessMode) -> unique_ptr<Catalog> {
			std::this_thread::sleep_for(std::chrono::seconds(5));
			return make_uniq_base<Catalog, DuckCatalog>(db);
		};
		create_transaction_manager = [](StorageExtensionInfo *, AttachedDatabase &db,
		                                Catalog &) -> unique_ptr<TransactionManager> {
			return make_uniq<DuckTransactionManager>(db);
		};
	}
};

TEST_CASE("Test db creation does not block instance cache", "[api]") {
	DBInstanceCache instance_cache;
	using namespace std::chrono;

	auto second_creation_was_quick = false;
	shared_ptr<DuckDB> stick_around;
	std::thread t1 {[&instance_cache, &second_creation_was_quick, &stick_around]() {
		DBConfig db_config;

		db_config.storage_extensions["delay"] = make_uniq<DelayingStorageExtension>();
		stick_around = instance_cache.GetOrCreateInstance("delay::memory:", db_config, true);

		const auto start_time = steady_clock::now();
		for (idx_t i = 0; i < 10; i++) {
			db_config.storage_extensions["delay"] = make_uniq<DelayingStorageExtension>();
			instance_cache.GetOrCreateInstance("delay::memory:", db_config, true);
		}
		const auto end_time = steady_clock::now();
		second_creation_was_quick = duration_cast<seconds>(end_time - start_time).count() < 1;
	}};
	std::this_thread::sleep_for(seconds(2));

	auto opening_slow_db_takes_remaining_time = false;
	std::thread t2 {[&instance_cache, &opening_slow_db_takes_remaining_time]() {
		DBConfig db_config;
		const auto start_time = steady_clock::now();
		instance_cache.GetOrCreateInstance("delay::memory:", db_config, true);
		const auto end_time = steady_clock::now();
		const auto duration = duration_cast<milliseconds>(end_time - start_time);
		opening_slow_db_takes_remaining_time = duration > seconds(2) && duration < seconds(4);
	}};

	auto no_delay_for_db_creation = true;
	std::thread t3 {[&instance_cache, &no_delay_for_db_creation]() {
		const auto start_time = steady_clock::now();
		DBConfig db_config;
		while (start_time + seconds(3) < steady_clock::now()) {
			auto db_start_time = steady_clock::now();
			instance_cache.GetOrCreateInstance(":memory:", db_config, false);
			no_delay_for_db_creation &= duration_cast<milliseconds>(steady_clock::now() - db_start_time).count() < 100;
		}
	}};

	t1.join();
	t2.join();
	t3.join();
	REQUIRE(second_creation_was_quick);
	REQUIRE(opening_slow_db_takes_remaining_time);
	REQUIRE(no_delay_for_db_creation);
}

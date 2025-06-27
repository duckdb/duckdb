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

TEST_CASE("Test db creation does not block instance cache", "[integration]") {
	DBInstanceCache instance_cache;

	auto second_creation_was_quick = false;
	std::thread t1 {[&instance_cache, &second_creation_was_quick]() {
		DBConfig db_config;

		db_config.storage_extensions["delay"] = make_uniq<DelayingStorageExtension>();
		auto stick_around = instance_cache.GetOrCreateInstance("delay::memory:", db_config, true);

		const auto start_time = high_resolution_clock::now();
		for (idx_t i = 0; i < 10; i++) {
			db_config.storage_extensions["delay"] = make_uniq<DelayingStorageExtension>();
			instance_cache.GetOrCreateInstance("delay::memory:", db_config, true);
		}
		const auto end_time = high_resolution_clock::now();
		second_creation_was_quick = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count() < 1;
	}};
	std::this_thread::sleep_for(std::chrono::seconds(1));

	auto shutdown_was_quick_enough = false;
	std::thread t2 {[&instance_cache, &shutdown_was_quick_enough]() {
		const auto start_time = high_resolution_clock::now();
		DBConfig db_config;
		instance_cache.GetOrCreateInstance(":memory:", db_config, false);
		const auto end_time = high_resolution_clock::now();
		shutdown_was_quick_enough = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count() < 1;
	}};

	t1.join();
	t2.join();
	REQUIRE(second_creation_was_quick);
	REQUIRE(shutdown_was_quick_enough);
}

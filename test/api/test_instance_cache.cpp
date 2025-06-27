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

void log_time(const std::string &message) {
	auto now = system_clock::now();
	auto now_time_t = system_clock::to_time_t(now);
	std::tm now_tm = *std::localtime(&now_time_t);
	char buffer[20];
	std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &now_tm);
	std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &now_tm);
	Printer::Print("[" + std::string(buffer) + "] " + message);
}

TEST_CASE("Test duckdb in memory database queued up after hanging md connection - with instance cache",
          "[integration]") {

	DBInstanceCache instance_cache;
	DBConfig db_config;
	db_config.storage_extensions["delay"] = make_uniq<DelayingStorageExtension>();

	std::thread t1 {[&] {
		instance_cache.GetOrCreateInstance("delay::memory:", db_config, true);
	}};
	std::this_thread::sleep_for(std::chrono::seconds(1));
	std::thread t2 {[&] {
		log_time("Create second DB");
		instance_cache.GetOrCreateInstance(":memory:", db_config, false);
		log_time("Finished creation");
	}};
	t1.join();
	t2.join();
}

#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/catalog/duck_catalog.hpp"

using namespace duckdb;

// Test to see if extensions can be loaded with their normal name and aliases.

struct DummyStorageExtension : StorageExtension {
	DummyStorageExtension() {
		attach = [](optional_ptr<StorageExtensionInfo>, ClientContext &, AttachedDatabase &db, const string &,
		            AttachInfo &info, AttachOptions &) -> unique_ptr<Catalog> {
			return make_uniq_base<Catalog, DuckCatalog>(db);
		};
		create_transaction_manager = [](optional_ptr<StorageExtensionInfo>, AttachedDatabase &db,
		                                Catalog &) -> unique_ptr<TransactionManager> {
			return make_uniq<DuckTransactionManager>(db);
		};
	}
};

struct DetachTrackingCatalog : DuckCatalog {
	DetachTrackingCatalog(AttachedDatabase &db, idx_t &detach_count_p) : DuckCatalog(db), detach_count(detach_count_p) {
	}

	void OnDetach(ClientContext &) override {
		detach_count++;
	}

	idx_t &detach_count;
};

struct DetachTrackingStorageExtensionInfo : StorageExtensionInfo {
	explicit DetachTrackingStorageExtensionInfo(idx_t &detach_count_p) : detach_count(detach_count_p) {
	}

	idx_t &detach_count;
};

struct DetachTrackingStorageExtension : StorageExtension {
	explicit DetachTrackingStorageExtension(idx_t &detach_count) {
		storage_info = make_shared_ptr<DetachTrackingStorageExtensionInfo>(detach_count);
		attach = [](optional_ptr<StorageExtensionInfo> storage_info, ClientContext &, AttachedDatabase &db,
		            const string &, AttachInfo &, AttachOptions &) -> unique_ptr<Catalog> {
			auto &tracking_info = static_cast<DetachTrackingStorageExtensionInfo &>(*storage_info);
			return make_uniq_base<Catalog, DetachTrackingCatalog>(db, tracking_info.detach_count);
		};
		create_transaction_manager = [](optional_ptr<StorageExtensionInfo>, AttachedDatabase &db,
		                                Catalog &) -> unique_ptr<TransactionManager> {
			return make_uniq<DuckTransactionManager>(db);
		};
	}
};

TEST_CASE("Test storage extension lookup full-name", "[api]") {
	DBConfig config;

	// Register a storage extension under its CANONICAL name "sqlite_scanner"
	// This mimics how the real sqlite_scanner extension registers itself
	// There is a hard-coded alias "sqlite" for "sqlite_scanner"
	StorageExtension::Register(config, "sqlite_scanner", make_shared_ptr<DummyStorageExtension>());

	DuckDB db(nullptr, &config);
	Connection con(db);

	// this works since it is the full name
	auto query = string("ATTACH ':memory:' AS db1 (TYPE SQLITE_SCANNER)");
	auto result = con.Query(query);
	if (result->HasError()) {
		FAIL("Query failed even though sqlite_scanner is registered."
		     "Query: " +
		     query + "\n" + "Error: " + result->GetError());
	}
}

TEST_CASE("Test storage extension lookup alias", "[api]") {
	DBConfig config;

	// Register a storage extension under its CANONICAL name "sqlite_scanner"
	// This mimics how the real sqlite_scanner extension registers itself
	// there is a hard-coded alias "sqlite" for "sqlite_scanner"
	StorageExtension::Register(config, "sqlite_scanner", make_shared_ptr<DummyStorageExtension>());

	DuckDB db(nullptr, &config);
	Connection con(db);

	// Without ApplyExtensionAlias in database_manager.cpp,
	// this fails with an error about not finding the extension
	auto query = string("ATTACH ':memory:' AS db1 (TYPE SQLITE)");
	auto result = con.Query(query);
	if (result->HasError()) {
		FAIL("Query failed even though sqlite_scanner is registered.\n"
		     "Query: " +
		     query + "\n" + "Error: " + result->GetError());
	}
}

TEST_CASE("Uncommitted attachment aliases cannot be reused", "[api]") {
	auto path_a = TestCreatePath("pending_attach_a.db");
	auto path_b = TestCreatePath("pending_attach_b.db");
	auto path_c = TestCreatePath("pending_attach_c.db");
	DuckDB db(nullptr);
	Connection owner(db);
	Connection other(db);

	REQUIRE_NO_FAIL(owner.Query("ATTACH '" + path_a + "' AS x"));
	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE x.marker AS SELECT 1 AS value"));
	REQUIRE_NO_FAIL(owner.Query("DETACH x"));
	REQUIRE_NO_FAIL(owner.Query("ATTACH '" + path_b + "' AS x"));
	REQUIRE_NO_FAIL(owner.Query("CREATE TABLE x.marker AS SELECT 2 AS value"));
	REQUIRE_NO_FAIL(owner.Query("DETACH x"));
	REQUIRE_NO_FAIL(owner.Query("ATTACH '" + path_a + "' AS x"));
	REQUIRE_NO_FAIL(owner.Query("BEGIN"));
	REQUIRE_NO_FAIL(owner.Query("ATTACH OR REPLACE '" + path_b + "' AS x"));

	REQUIRE(other.Query("DETACH x")->HasError());
	REQUIRE(other.Query("ALTER DATABASE x SET ALIAS TO y")->HasError());
	REQUIRE(other.Query("ATTACH OR REPLACE '" + path_c + "' AS x")->HasError());
	REQUIRE_NO_FAIL(owner.Query("ROLLBACK"));
	auto result = owner.Query("SELECT value FROM x.marker");
	REQUIRE(!result->HasError());
	REQUIRE(result->GetValue<int32_t>(0, 0) == 1);
}

TEST_CASE("Replaced catalogs detach after a later commit failure", "[api]") {
	idx_t detach_count = 0;
	DBConfig config;
	StorageExtension::Register(config, "detach_tracking",
	                           make_shared_ptr<DetachTrackingStorageExtension>(detach_count));
	DuckDB db(nullptr, &config);
	Connection con(db);
	auto old_path = TestCreatePath("detach_tracking_old.db");
	auto new_path = TestCreatePath("detach_tracking_new.db");

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE commit_failure(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + old_path + "' AS x (TYPE detach_tracking)"));
	REQUIRE_NO_FAIL(con.Query("SET debug_force_commit_failure=true"));
	REQUIRE_NO_FAIL(con.Query("BEGIN"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO commit_failure VALUES (1)"));
	REQUIRE_NO_FAIL(con.Query("ATTACH OR REPLACE '" + new_path + "' AS x (TYPE detach_tracking)"));
	auto result = con.Query("COMMIT");
	REQUIRE(result->HasError());
	REQUIRE(detach_count == 1);
}

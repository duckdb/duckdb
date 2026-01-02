#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_manager.hpp"

#include "duckdb.hpp"

using namespace duckdb;

void SomeTableFunc(duckdb::ClientContext &, duckdb::TableFunctionInput &, duckdb::DataChunk &) {
}

TEST_CASE("Test catalog versioning", "[catalog]") {
	DBConfig config;
	config.options.allow_unsigned_extensions = true;
	DuckDB db(nullptr, &config);
	Connection con1(db);

	con1.context->RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*con1.context, "");
		REQUIRE(catalog.GetCatalogVersion(*con1.context) == 0);
	});

	REQUIRE_NO_FAIL(con1.Query("CREATE TABLE foo as SELECT 42"));

	con1.context->RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*con1.context, "");
		REQUIRE(catalog.GetCatalogVersion(*con1.context) == 1);
	});

	{
		REQUIRE_NOTHROW(con1.BeginTransaction());
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE foo2 as SELECT 42"));
		auto &catalog = Catalog::GetCatalog(*con1.context, "");
		auto catalog_version_1 = catalog.GetCatalogVersion(*con1.context);
		REQUIRE(catalog_version_1.GetIndex() > TRANSACTION_ID_START);
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE foo3 as SELECT 42"));
		auto catalog_version_2 = catalog.GetCatalogVersion(*con1.context);
		REQUIRE(catalog_version_2.GetIndex() > catalog_version_1.GetIndex());
		REQUIRE_NOTHROW(con1.Commit());
	}

	con1.context->RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*con1.context, "");
		REQUIRE(catalog.GetCatalogVersion(*con1.context) == 2);
	});

	// The following should not increment the version
	REQUIRE_NO_FAIL(con1.Query("CREATE TABLE IF NOT EXISTS foo3 as SELECT 42"));

	con1.context->RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*con1.context, "");
		REQUIRE(catalog.GetCatalogVersion(*con1.context) == 2);
	});

	REQUIRE_NO_FAIL(con1.Query("CREATE SCHEMA IF NOT EXISTS my_schema"));

	con1.context->RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*con1.context, "");
		REQUIRE(catalog.GetCatalogVersion(*con1.context) == 3);
	});

	// The following should not increment the version
	REQUIRE_NO_FAIL(con1.Query("CREATE SCHEMA IF NOT EXISTS my_schema"));

	con1.context->RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*con1.context, "");
		REQUIRE(catalog.GetCatalogVersion(*con1.context) == 3);
	});

	// check catalog version of system catalog
	con1.context->RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*con1.context, "system");
		REQUIRE(catalog.GetCatalogVersion(*con1.context) == 1);
	});
	REQUIRE_NO_FAIL(con1.Query("ATTACH ':memory:' as foo"));
	con1.context->RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*con1.context, "system");
		REQUIRE(catalog.GetCatalogVersion(*con1.context) == 2);
	});

	// check new version of attached DB starts at 0
	con1.context->RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*con1.context, "foo");
		REQUIRE(catalog.GetCatalogVersion(*con1.context) == 0);
	});

	// system transactions do not register catalog version changes :/
	duckdb::TableFunction tf("some_new_table_function", {}, SomeTableFunc);
	ExtensionInfo extension_info {};
	ExtensionActiveLoad load_info {*db.instance, extension_info, "test_catalog_extension"};
	ExtensionLoader loader {load_info};
	loader.RegisterFunction(tf);

	con1.context->RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*con1.context, "system");
		REQUIRE(catalog.GetCatalogVersion(*con1.context) == 2);
	});
}

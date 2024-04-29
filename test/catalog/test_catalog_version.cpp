#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"

using namespace duckdb;

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
		REQUIRE(catalog_version_1 > catalog.UNCOMMITTED_CATALOG_VERSION_START);
		REQUIRE_NO_FAIL(con1.Query("CREATE TABLE foo3 as SELECT 42"));
		auto catalog_version_2 = catalog.GetCatalogVersion(*con1.context);
		REQUIRE(catalog_version_2 > catalog_version_1);
		REQUIRE_NOTHROW(con1.Commit());
	}

	con1.context->RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*con1.context, "");
		REQUIRE(catalog.GetCatalogVersion(*con1.context) == 2);
	});
}


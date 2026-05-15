#include "catch.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_manager.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/client_data.hpp"

using namespace duckdb;

static void ReturnCustomSchema(DataChunk &args, ExpressionState &, Vector &result) {
	result.Reference(Value("custom_schema"), count_t(args.size()));
}

static void ReturnMainSchema(DataChunk &args, ExpressionState &, Vector &result) {
	result.Reference(Value("main_schema"), count_t(args.size()));
}

static ExtensionLoader CreateExtensionLoader(DuckDB &db, const string &name) {
	ExtensionInfo extension_info {};
	ExtensionActiveLoad load_info {*db.instance, extension_info, name, ""};
	return ExtensionLoader {load_info};
}

TEST_CASE("Test ExtensionLoader schema API", "[api]") {
	DuckDB db(nullptr);
	Connection conn(db);

	SECTION("CreateExtensionSchema creates a schema visible in the catalog") {
		auto loader = CreateExtensionLoader(db, "test_ext");
		loader.CreateExtensionSchema("test_schema");

		auto result =
		    conn.Query("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'test_schema'");
		REQUIRE_NO_FAIL(*result);
		REQUIRE(result->RowCount() == 1);
	}

	SECTION("SetExtensionSchema routes RegisterFunction into the custom schema") {
		auto loader = CreateExtensionLoader(db, "test_ext");
		loader.CreateExtensionSchema("custom_schema");
		loader.SetExtensionSchema("custom_schema");
		loader.RegisterFunction(ScalarFunction("fn_in_custom", {}, LogicalType::VARCHAR, ReturnCustomSchema));

		// Both qualified and unqualified call fail because schema is not on the search path yet
		REQUIRE_FAIL(conn.Query("SELECT custom_schema.fn_in_custom()"));
		REQUIRE_FAIL(conn.Query("SELECT fn_in_custom()"));
	}

	SECTION("Test AddExtensionSchemaToSearchPath") {
		auto loader = CreateExtensionLoader(db, "test_ext");
		loader.CreateExtensionSchema("search_schema");
		loader.SetExtensionSchema("search_schema");
		loader.AddExtensionSchemaToSearchPath("search_schema");
		loader.RegisterFunction(ScalarFunction("fn_in_search", {}, LogicalType::VARCHAR, ReturnCustomSchema));

		// we need to manually call this to refresh the catalog search path for this connection
		// normally this is done in physical_load.cpp, after the extension is successfully loaded
		loader.RefreshSearchPath(*conn.context);

		REQUIRE_NO_FAIL(conn.Query("SELECT search_schema.fn_in_search()"));
		REQUIRE_NO_FAIL(conn.Query("SELECT fn_in_search()"));
	}

	SECTION("ResetExtensionSchemaToDefault routes subsequent functions back to main") {
		auto loader = CreateExtensionLoader(db, "test_ext");
		loader.CreateExtensionSchema("reset_schema");
		loader.SetExtensionSchema("reset_schema");
		loader.RegisterFunction(ScalarFunction("fn_before_reset", {}, LogicalType::VARCHAR, ReturnCustomSchema));

		// reset_schema has not been added to the search path yet
		REQUIRE_FAIL(conn.Query("SELECT reset_schema.fn_before_reset()"));
		loader.AddExtensionSchemaToSearchPath("reset_schema");
		loader.RefreshSearchPath(*conn.context);

		loader.ResetExtensionSchemaToDefault();
		// register another function in the main schema
		loader.RegisterFunction(ScalarFunction("fn_after_reset", {}, LogicalType::VARCHAR, ReturnMainSchema));

		// function registered in reset_schema
		REQUIRE_NO_FAIL(conn.Query("SELECT reset_schema.fn_before_reset()"));
		// check a function that is registered in the main schema
		REQUIRE_NO_FAIL(conn.Query("SELECT main.fn_after_reset()"));
		// fn_before_reset is not in the default schema
		REQUIRE_FAIL(conn.Query("SELECT main.fn_before_reset()"));
		// fn_after_reset is not in reset_schema
		REQUIRE_FAIL(conn.Query("SELECT reset_schema.fn_after_reset()"));
	}

	SECTION("SetExtensionSchema rejects invalid schema names") {
		auto loader = CreateExtensionLoader(db, "test_ext");
		REQUIRE_THROWS(loader.SetExtensionSchema("pg_catalog"));
		REQUIRE_THROWS(loader.SetExtensionSchema(""));
	}

	SECTION("AddExtensionSchemaToSearchPath requires SetExtensionSchema and CreateExtensionSchema first") {
		auto loader = CreateExtensionLoader(db, "test_ext");
		// schema does not exist
		REQUIRE_THROWS(loader.AddExtensionSchemaToSearchPath("orphan_schema"));
		// create schema
		loader.CreateExtensionSchema("orphan_schema");
		// still throws because schema is not set
		REQUIRE_THROWS(loader.AddExtensionSchemaToSearchPath("orphan_schema"));
	}

	SECTION("RegisterExtensionInSeparateSchema combines create, set, and add-to-path") {
		auto loader = CreateExtensionLoader(db, "test_ext");
		loader.RegisterExtensionInSeparateSchema("combined_schema");
		loader.RegisterFunction(ScalarFunction("fn_combined", {}, LogicalType::VARCHAR, ReturnCustomSchema));
		loader.RefreshSearchPath(*conn.context);

		REQUIRE_NO_FAIL(conn.Query("SELECT combined_schema.fn_combined()"));
		REQUIRE_NO_FAIL(conn.Query("SELECT fn_combined()"));
	}
}

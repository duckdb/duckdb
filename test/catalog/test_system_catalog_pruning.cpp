#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/table/system_catalog_pruning.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"

using namespace duckdb;

TEST_CASE("SystemCatalogPruning::CreateBindData locates identity columns", "[system_catalog_pruning]") {
	// Column order mirrors duckdb_tables(): database_name, database_oid, schema_name, schema_oid, table_name, ...
	vector<Identifier> names {"database_name", "database_oid", "schema_name", "schema_oid", "table_name"};
	auto bind_data = SystemCatalogPruning::CreateBindData(names);
	REQUIRE(bind_data);
	auto &data = bind_data->Cast<SystemCatalogPruningData>();
	REQUIRE(data.database_name_idx.IsValid());
	REQUIRE(data.database_name_idx.GetIndex() == 0);
	REQUIRE(data.database_oid_idx.GetIndex() == 1);
	REQUIRE(data.schema_name_idx.GetIndex() == 2);
	REQUIRE(data.schema_oid_idx.GetIndex() == 3);
	REQUIRE(data.predicates.empty());

	// A function missing some identity columns (e.g. duckdb_functions has no schema_oid) leaves those invalid.
	vector<Identifier> partial {"database_name", "database_oid", "schema_name"};
	auto partial_data = SystemCatalogPruning::CreateBindData(partial);
	auto &pd = partial_data->Cast<SystemCatalogPruningData>();
	REQUIRE(pd.schema_name_idx.IsValid());
	REQUIRE_FALSE(pd.schema_oid_idx.IsValid());
	REQUIRE_FALSE(pd.IsIdentityColumn(4)); // table_name is not an identity column
	REQUIRE(pd.IsIdentityColumn(2));       // schema_name is
}

TEST_CASE("SystemCatalogPruning::GetSchemas with no predicates equals GetAllSchemas", "[system_catalog_pruning]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("ATTACH ':memory:' AS db1"));
	REQUIRE_NO_FAIL(con.Query("ATTACH ':memory:' AS db2"));
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA db1.s1"));
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA db2.s2"));

	con.context->RunFunctionInTransaction([&]() {
		auto &context = *con.context;

		// Bind data with no predicates must fall back to the exact GetAllSchemas list and order.
		vector<Identifier> names {"database_name", "database_oid", "schema_name", "schema_oid"};
		auto bind_data = SystemCatalogPruning::CreateBindData(names);

		SystemCatalogPruningStats stats;
		auto pruned = SystemCatalogPruning::GetSchemas(context, bind_data.get(), stats);
		auto all = Catalog::GetAllSchemas(context);

		REQUIRE(pruned.size() == all.size());
		for (idx_t i = 0; i < all.size(); i++) {
			REQUIRE(&pruned[i].get() == &all[i].get());
		}

		// A null bind_data is the same fallback.
		SystemCatalogPruningStats stats2;
		auto pruned_null = SystemCatalogPruning::GetSchemas(context, nullptr, stats2);
		REQUIRE(pruned_null.size() == all.size());
	});
}

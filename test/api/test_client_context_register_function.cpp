#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

// ClientContext::RegisterFunction is a C++ registration API, and SQL cannot create schemas in the system catalog

static void ReturnInteger(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(Value::INTEGER(1), count_t(args.size()));
}

static idx_t OverloadCount(Connection &con, const QualifiedName &name) {
	con.BeginTransaction();
	auto &entry = Catalog::GetEntry<ScalarFunctionCatalogEntry>(*con.context, name);
	auto count = entry.functions.Size();
	con.Rollback();
	return count;
}

TEST_CASE("ClientContext::RegisterFunction merges with the function under the whole schema path", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &system_catalog = Catalog::GetSystemCatalog(*db.instance);
	auto data = CatalogTransaction::GetSystemTransaction(*db.instance);
	CreateSchemaInfo outer_schema;
	outer_schema.SetQualifiedName(QualifiedName::FromPath({Identifier("system"), Identifier("s"), Identifier()}));
	outer_schema.internal = true;
	system_catalog.CreateSchema(data, outer_schema);
	CreateSchemaInfo nested_schema;
	nested_schema.SetQualifiedName(
	    QualifiedName::FromPath({Identifier("system"), Identifier("s"), Identifier::DefaultSchema(), Identifier()}));
	nested_schema.internal = true;
	system_catalog.CreateSchema(data, nested_schema);

	CreateScalarFunctionInfo main_info(
	    ScalarFunction("ctx_fun", vector<LogicalType> {LogicalType::INTEGER}, LogicalType::INTEGER, ReturnInteger));
	con.context->RegisterFunction(main_info);

	// a set registered in system.s.main is merged with the function there, not with system.main's function of that
	// name, whose schema is also called main
	CreateScalarFunctionInfo nested_info(
	    ScalarFunction("ctx_fun", vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::INTEGER, ReturnInteger));
	nested_info.SetQualifiedName(
	    QualifiedName::FromPath({Identifier("system"), Identifier("s"), Identifier("main"), Identifier("ctx_fun")}));
	con.context->RegisterFunction(nested_info);
	const auto nested_name =
	    QualifiedName::FromPath({Identifier("system"), Identifier("s"), Identifier("main"), Identifier("ctx_fun")});
	const auto main_name = QualifiedName(Identifier("system"), Identifier::DefaultSchema(), Identifier("ctx_fun"));
	REQUIRE(OverloadCount(con, nested_name) == 1);
	REQUIRE(OverloadCount(con, main_name) == 1);

	// a second overload for system.main merges with the existing one there
	CreateScalarFunctionInfo main_overload(
	    ScalarFunction("ctx_fun", vector<LogicalType> {LogicalType::BIGINT, LogicalType::BIGINT}, LogicalType::INTEGER,
	                   ReturnInteger));
	con.context->RegisterFunction(main_overload);
	REQUIRE(OverloadCount(con, main_name) == 2);
	REQUIRE(OverloadCount(con, nested_name) == 1);
	auto result = con.Query("SELECT ctx_fun(1), ctx_fun(1::BIGINT, 2::BIGINT)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
}

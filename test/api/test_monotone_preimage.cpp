#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

using namespace duckdb;

static void MonotoneIdentity(DataChunk &args, ExpressionState &, Vector &result) {
	result.Reference(args.data[0]);
}

TEST_CASE("Rounding preimages do not apply to same-named user functions", "[api][monotone_preimage]") {
	for (const auto &type : vector<LogicalType> {LogicalType::FLOAT, LogicalType::DOUBLE}) {
		CAPTURE(type.ToString());
		DuckDB db(nullptr);
		Connection con(db);
		con.BeginTransaction();
		REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA custom"));
		for (const auto &name : {"floor", "ceil", "ceiling", "trunc"}) {
			ScalarFunction function(name, {type}, type, MonotoneIdentity);
			function.SetUnaryArgProperties(ArgProperties().StrictlyIncreasing());
			CreateScalarFunctionInfo info(function);
			info.internal = false;
			info.SetSchema("custom");
			con.context->RunFunctionInTransaction(
			    [&] { Catalog::GetCatalog(*con.context, "memory").CreateFunction(*con.context, info); });
		}
		con.Commit();
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(d " + type.ToString() + ")"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1.5), (2), (NULL)"));
		REQUIRE_NO_FAIL(con.Query("SET search_path='custom,main'"));
		for (const auto &name : {"floor", "ceil", "ceiling", "trunc"}) {
			CAPTURE(name);
			for (const auto &prefix : {"custom.", ""}) {
				CAPTURE(prefix);
				auto result = con.Query("SELECT d FROM t WHERE " + string(prefix) + name + "(d) = 1.5");
				REQUIRE_NO_FAIL(*result);
				REQUIRE(result->RowCount() == 1);
				REQUIRE(result->Collection().GetValue(0, 0).GetValue<double>() == 1.5);

				result = con.Query("SELECT d FROM t WHERE " + string(prefix) + name + "(d) >= 2");
				REQUIRE_NO_FAIL(*result);
				REQUIRE(result->RowCount() == 1);
				REQUIRE(result->Collection().GetValue(0, 0).GetValue<double>() == 2);
			}
			auto result = con.Query("SELECT d FROM t WHERE system.main." + string(name) + "(d) = 1.5");
			REQUIRE_NO_FAIL(*result);
			REQUIRE(result->RowCount() == 0);
		}
		for (const auto &name : {"floor", "ceil", "ceiling", "trunc"}) {
			CAPTURE(name);
			ScalarFunction function(name, {type}, type, MonotoneIdentity);
			function.SetUnaryArgProperties(ArgProperties().StrictlyIncreasing());
			CreateScalarFunctionInfo info(function);
			info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
			con.context->RunFunctionInTransaction(
			    [&] { Catalog::GetSystemCatalog(*con.context).CreateFunction(*con.context, info); });
			auto result = con.Query("SELECT d FROM t WHERE system.main." + string(name) + "(d) = 1.5");
			REQUIRE_NO_FAIL(*result);
			REQUIRE(result->RowCount() == 1);
			REQUIRE(result->Collection().GetValue(0, 0).GetValue<double>() == 1.5);
		}
	}
}

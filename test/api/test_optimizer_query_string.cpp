#include "catch.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

// Captures the query string seen by the optimizer extension callback
static string captured_query;
static int capture_count;

static void CaptureQueryPreOptimize(OptimizerExtensionInput &input, duckdb::unique_ptr<LogicalOperator> &plan) {
	if (input.query) {
		captured_query = *input.query;
	} else {
		captured_query = "";
	}
	capture_count++;
}

TEST_CASE("Optimizer extension receives query string for regular and prepared statements", "[api]") {
	DuckDB db;
	auto &config = DBConfig::GetConfig(*db.instance);

	OptimizerExtension ext;
	ext.pre_optimize_function = CaptureQueryPreOptimize;
	OptimizerExtension::Register(config, std::move(ext));

	Connection con(*db.instance);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t (i INTEGER, s VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO t VALUES (1, 'hello'), (2, 'world')"));

	SECTION("Regular query sets the query string") {
		captured_query.clear();
		string query = "SELECT * FROM t WHERE i > 0";
		auto result = con.Query(query);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(!captured_query.empty());
		REQUIRE(captured_query.find("SELECT") != string::npos);
	}

	SECTION("Prepared statement without parameters sets the query string") {
		captured_query.clear();
		string query = "SELECT * FROM t";
		auto prepared = con.Prepare(query);
		REQUIRE(!prepared->HasError());
		// Optimizer runs during Prepare for parameter-free statements
		REQUIRE(!captured_query.empty());
		REQUIRE(captured_query.find("SELECT") != string::npos);
	}

	SECTION("Prepared statement with parameters sets the query string on Execute") {
		captured_query.clear();
		capture_count = 0;
		string query = "SELECT * FROM t WHERE i = $1";
		auto prepared = con.Prepare(query);
		REQUIRE(!prepared->HasError());
		// Optimizer is deferred until Execute when parameters are unbound
		auto result = prepared->Execute(1);
		REQUIRE_NO_FAIL(*result);
		REQUIRE(!captured_query.empty());
		REQUIRE(captured_query.find("SELECT") != string::npos);
	}
}

#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api.hpp"

#include <string>

// ---------------------------------------------------------------------------
// Stable C++ API tests: QueryMetrics. A snapshot of the metrics of the last
// query a connection finished.
// ---------------------------------------------------------------------------

namespace {

using namespace duckdb::cxx;

// The first node of the given name, searching depth-first.
std::optional<QueryMetricsNode> FindOperator(const QueryMetricsNode &node, std::string_view name) {
	for (idx_t i = 0; i < node.GetChildCount(); i++) {
		auto child = node.GetChild(i);
		if (child.GetName() == name) {
			return child;
		}
		auto found = FindOperator(child, name);
		if (found) {
			return found;
		}
	}
	return std::nullopt;
}

} // namespace

TEST_CASE("Stable C++API: query metrics", "[cpp_api]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();

	auto empty = conn.GetQueryMetrics();
	REQUIRE(empty.GetRoot().GetName() == "QUERY");
	REQUIRE(empty.GetRoot().GetMetricCount() == 0);
	REQUIRE(empty.GetRoot().GetChildCount() == 0);

	// Disable first, so enabling keeps the format the setting chose: no output.
	conn.Execute("SET enable_profiling = 'no_output'").Drain();
	conn.EnableProfiling(false);
	conn.Execute("SELECT 42").Drain();
	REQUIRE(conn.GetQueryMetrics().GetRoot().GetChildCount() == 0);
	conn.EnableProfiling();
	conn.Execute("SELECT sum(i) FROM range(1000) t(i)").Drain();

	auto metrics = conn.GetQueryMetrics();
	auto root = metrics.GetRoot();
	REQUIRE(root.GetMetric("query.total_time").has_value());
	REQUIRE_FALSE(root.GetMetric("query.sql").has_value());
	for (idx_t i = 0; i < root.GetMetricCount(); i++) {
		REQUIRE(root.GetMetric(root.GetMetricName(i)).has_value());
		REQUIRE(root.GetMetricValue(i).Get<double>() >= 0);
	}
	REQUIRE_THROWS_MATCHES(root.GetChild(root.GetChildCount()), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE));

	auto scan = FindOperator(root, "TABLE_SCAN");
	REQUIRE(scan.has_value());
	auto rows = scan->GetMetric("operator.intermediate_rows");
	REQUIRE(rows.has_value());
	REQUIRE(rows->Get<uint64_t>() == 1000);
}

TEST_CASE("Stable C++API: query metrics need a finished query", "[cpp_api]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();
	auto result = conn.Execute("SELECT * FROM range(100000)");
	result.FetchChunk();
	REQUIRE_THROWS_MATCHES(conn.GetQueryMetrics(), Exception, HasErrorCode(DUCKDB_V2_ERROR_RESOURCE_IN_USE));
	result.Drain();
	REQUIRE_NOTHROW(conn.GetQueryMetrics());
}

TEST_CASE("Stable C++API: GetProfilingInformation renders the profile of the last query", "[cpp_api]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();

	// Without profiling there is no profile to render
	auto disabled = conn.GetProfilingInformation("json");

	conn.Execute("SET enable_profiling = 'no_output'").Drain();
	conn.Execute("SELECT sum(i) FROM range(1000) t(i)").Drain();

	auto json = conn.GetProfilingInformation("json");
	REQUIRE(json.find("UNGROUPED_AGGREGATE") != std::string::npos);
	REQUIRE(json != disabled);
	REQUIRE(conn.GetProfilingInformation("text").find("Ungrouped Aggregate") != std::string::npos);
	// The empty format is the configured one, which falls back to the query tree when it is no_output
	REQUIRE(conn.GetProfilingInformation() == conn.GetProfilingInformation("query_tree"));
	REQUIRE(conn.GetProfilingInformation("no_output").empty());

	REQUIRE_THROWS_MATCHES(conn.GetProfilingInformation("no_such_format"), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// Like the metrics, the profile is of a finished query
	auto result = conn.Execute("SELECT * FROM range(100000)");
	result.FetchChunk();
	REQUIRE_THROWS_MATCHES(conn.GetProfilingInformation("json"), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_RESOURCE_IN_USE));
	result.Drain();
	REQUIRE_NOTHROW(conn.GetProfilingInformation("json"));
}

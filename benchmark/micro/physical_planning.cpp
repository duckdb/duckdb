#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;

DUCKDB_BENCHMARK(PhysicalPlanManyCTEs, "[optimizer]")
private:
static string CreateQuery() {
	string query = "PREPARE physical_plan_properties AS ";
	for (idx_t i = 0; i < 8192; i++) {
		if (i > 0) {
			query += " UNION ALL ";
		}
		query += "SELECT sum(i) total FROM range(" + to_string(8 + i / 2) + ") r(i)";
	}
	return query;
}

const string query = CreateQuery();

public:
void Load(DuckDBBenchmarkState *state) override {
	auto result = state->conn.Query("SET max_expression_depth=10000");
	if (result->HasError()) {
		throw InvalidInputException(result->GetError());
	}
}

string GetQuery() override {
	// Prepare the generated SQL directly: query(getvariable(...)) defers physical planning.
	return query;
}

string VerifyResult(QueryResult *result) override {
	return result->HasError() ? result->GetError() : string();
}

string BenchmarkInfo() override {
	return "Prepare 4096 distinct aggregate subplans, each used twice and extracted into a materialized CTE.";
}
FINISH_BENCHMARK(PhysicalPlanManyCTEs)

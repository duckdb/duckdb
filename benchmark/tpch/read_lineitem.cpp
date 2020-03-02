#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 0.1

DUCKDB_BENCHMARK(ReadLineitemCSV, "[csv]")
int64_t count = 0;
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	state->conn.Query("CREATE SCHEMA tpch");
	tpch::dbgen(SF, state->db, "tpch");
	// create the CSV file
	auto result = state->conn.Query("COPY tpch.lineitem TO 'lineitem.csv' DELIMITER '|' HEADER");
	assert(result->success);
	count = result->collection.chunks[0]->GetValue(0, 0).GetValue<int64_t>();
	// delete the database
	state->conn.Query("DROP SCHEMA tpch CASCADE");
	// create the empty schema to load into
	tpch::dbgen(0, state->db);
}
string GetQuery() override {
	return "COPY lineitem FROM 'lineitem.csv' DELIMITER '|' HEADER";
}
void Cleanup(DuckDBBenchmarkState *state) override {
	state->conn.Query("DROP TABLE lineitem");
	tpch::dbgen(0, state->db);
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	auto expected_count = materialized.collection.chunks[0]->GetValue(0, 0).GetValue<int64_t>();
	if (expected_count != count) {
		return StringUtil::Format("Count mismatch, expected %lld elements but got %lld", count, expected_count);
	}
	return string();
}
string BenchmarkInfo() override {
	return "Read the lineitem table from SF 0.1 from CSV format";
}
FINISH_BENCHMARK(ReadLineitemCSV)

DUCKDB_BENCHMARK(ReadLineitemCSVUnicode, "[csv]")
int64_t count = 0;
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	state->conn.Query("CREATE SCHEMA tpch");
	tpch::dbgen(SF, state->db, "tpch");
	// create the CSV file
	auto result = state->conn.Query("COPY tpch.lineitem TO 'lineitem_unicode.csv' DELIMITER '🦆' HEADER");
	assert(result->success);
	count = result->collection.chunks[0]->GetValue(0, 0).GetValue<int64_t>();
	// delete the database
	state->conn.Query("DROP SCHEMA tpch CASCADE");
	// create the empty schema to load into
	tpch::dbgen(0, state->db);
}
string GetQuery() override {
	return "COPY lineitem FROM 'lineitem_unicode.csv' DELIMITER '🦆' HEADER";
}
void Cleanup(DuckDBBenchmarkState *state) override {
	state->conn.Query("DROP TABLE lineitem");
	tpch::dbgen(0, state->db);
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	auto expected_count = materialized.collection.chunks[0]->GetValue(0, 0).GetValue<int64_t>();
	if (expected_count != count) {
		return StringUtil::Format("Count mismatch, expected %lld elements but got %lld", count, expected_count);
	}
	return string();
}
string BenchmarkInfo() override {
	return "Read the lineitem table from SF 0.1 from CSV format";
}
FINISH_BENCHMARK(ReadLineitemCSVUnicode)

DUCKDB_BENCHMARK(WriteLineitemCSV, "[csv]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "COPY lineitem TO 'lineitem.csv' DELIMITER '|' HEADER";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Write the lineitem table from SF 0.1 to CSV format";
}
FINISH_BENCHMARK(WriteLineitemCSV)

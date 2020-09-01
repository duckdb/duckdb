// #include "benchmark_runner.hpp"
// #include "compare_result.hpp"
// #include "dbgen.hpp"
// #include "duckdb_benchmark_macro.hpp"
// #include "parquet-extension.hpp"

// using namespace duckdb;
// using namespace std;

// #define SF 1

// DUCKDB_BENCHMARK(LineitemParquet, "[parquet]")
// void Load(DuckDBBenchmarkState *state) override {
// 	state->db.LoadExtension<ParquetExtension>();
// 	auto res = state->conn.Query("CREATE OR REPLACE VIEW lineitem AS SELECT * FROM "
// 	                             "parquet_scan('extension/parquet/test/lineitem-sf1.uncompressed.parquet')");
// }
// string GetQuery() override {
// 	return tpch::get_query(1);
// }
// string VerifyResult(QueryResult *result) override {
// 	if (!result->success) {
// 		return result->error;
// 	}
// 	return string();
// }
// string BenchmarkInfo() override {
// 	return "Execute TPCH SF1 on a Parquet file";
// }
// FINISH_BENCHMARK(LineitemParquet)

// DUCKDB_BENCHMARK(LineitemParquetProjectionDirect, "[parquet]")
// void Load(DuckDBBenchmarkState *state) override {
// 	state->db.LoadExtension<ParquetExtension>();
// }
// string GetQuery() override {
// 	return "SELECT SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) FROM "
// 	       "parquet_scan('extension/parquet/test/lineitem-sf1.uncompressed.parquet')";
// }
// string VerifyResult(QueryResult *result) override {
// 	if (!result->success) {
// 		return result->error;
// 	}
// 	return string();
// }
// string BenchmarkInfo() override {
// 	return "Execute a simple query with projection on Parquet file directly on the function";
// }
// FINISH_BENCHMARK(LineitemParquetProjectionDirect)

// DUCKDB_BENCHMARK(LineitemParquetProjectionView, "[parquet]")
// void Load(DuckDBBenchmarkState *state) override {
// 	state->db.LoadExtension<ParquetExtension>();
// 	auto res = state->conn.Query("CREATE OR REPLACE VIEW lineitem AS SELECT * FROM "
// 	                             "parquet_scan('extension/parquet/test/lineitem-sf1.uncompressed.parquet')");
// }
// string GetQuery() override {
// 	return "SELECT SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) FROM lineitem";
// }
// string VerifyResult(QueryResult *result) override {
// 	if (!result->success) {
// 		return result->error;
// 	}
// 	return string();
// }
// string BenchmarkInfo() override {
// 	return "Execute a simple query with projection on Parquet file through a view";
// }
// FINISH_BENCHMARK(LineitemParquetProjectionView)

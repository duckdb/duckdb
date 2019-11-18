#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

void LoadData(DuckDBBenchmarkState *state) {
	Connection con(state->db);
	con.Query("CREATE TABLE nice_data (index INTEGER, str1 VARCHAR(20), str2 VARCHAR(11), str3 VARCHAR, int1 INTEGER, int2 INTEGER, date1 DATE, date2 DATE, bool1 BOOLEAN, double1 DOUBLE, bigint1 BIGINT);");
	auto result = con.Query("COPY nice_data FROM 'data.csv';");
	if (!result->success) {
		throw Exception(result->error);
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	if (materialized.collection.count != 1) {
		throw Exception("Error while copying!");
	}
}

string VerifyQueryResult(QueryResult *result) {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	if (materialized.collection.count == 0) {
		throw Exception("Wrong result size!");
	}
	return string();
}

DUCKDB_BENCHMARK(ExpressionReorderingQ1, "[expression_reordering]")
void Load(DuckDBBenchmarkState *state) override {
	LoadData(state);
}
string GetQuery() override {
	return "SELECT * FROM nice_data WHERE str1 IN ('shine', 'sunglasses', 'sunny', 'sunburn') AND int1 BETWEEN 1 AND 5 AND length(str2) != 5 AND date1 < cast('2004-01-01' as date) AND regexp_matches(str3, '.*sh.*') AND date2 IS NULL;";
}
string VerifyResult(QueryResult *result) override {
	return VerifyQueryResult(result);
}
string BenchmarkInfo() override {
	return "Execute Expression Reordering Q1";
}
FINISH_BENCHMARK(ExpressionReorderingQ1)


DUCKDB_BENCHMARK(ExpressionReorderingQ2, "[expression_reordering]")
void Load(DuckDBBenchmarkState *state) override {
	LoadData(state);
}
string GetQuery() override {
	return "SELECT * FROM nice_data WHERE bool1 AND int2 % 2 = 0 AND 2 * int2 >= 10 AND abs(double1 * -2.356323) >= 5.0 AND bigint1 / 10 <= 50000000000000000;";
}
string VerifyResult(QueryResult *result) override {
	return VerifyQueryResult(result);
}
string BenchmarkInfo() override {
	return "Execute Expression Reordering Q2";
}
FINISH_BENCHMARK(ExpressionReorderingQ2)


DUCKDB_BENCHMARK(ExpressionReorderingQ3, "[expression_reordering]")
void Load(DuckDBBenchmarkState *state) override {
	LoadData(state);
}
string GetQuery() override {
	return "SELECT * FROM nice_data t1 WHERE str1 LIKE '%shi%' AND regexp_matches(str1, '.*s.*') AND str2 = 'xxxxxxx' AND str1 IN ('shine', 'sunglasses', 'sunny', 'sunburn');";
}
string VerifyResult(QueryResult *result) override {
	return VerifyQueryResult(result);
}
string BenchmarkInfo() override {
	return "Execute Expression Reordering Q3";
}
FINISH_BENCHMARK(ExpressionReorderingQ3)


DUCKDB_BENCHMARK(ExpressionReorderingQ4, "[expression_reordering]")
void Load(DuckDBBenchmarkState *state) override {
	LoadData(state);
}
string GetQuery() override {
	return "SELECT * FROM nice_data t1 WHERE str1 = 'sunshine' AND int1 % 3 == 0;";
}
string VerifyResult(QueryResult *result) override {
	return VerifyQueryResult(result);
}
string BenchmarkInfo() override {
	return "Execute Expression Reordering Q4";
}
FINISH_BENCHMARK(ExpressionReorderingQ4)
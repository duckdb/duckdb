#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

#include <random>
#include <algorithm>

using namespace duckdb;
using namespace std;

DUCKDB_BENCHMARK(HashJoinBenno, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE words(index INTEGER, doc INTEGER, word VARCHAR);");
	state->conn.Query("COPY words FROM 'benchmark/micro/index/indexjoin.csv.gz' (DELIMITER ',' , AUTO_DETECT FALSE)");
}

virtual string GetQuery() {
	return "SELECT w2.doc as doc2, COUNT(*) AS c FROM words AS w1 JOIN words AS w2 ON "
	       "(w1.word=w2.word) GROUP BY doc2 ORDER BY c DESC LIMIT 10";
}

virtual string VerifyResult(QueryResult *result) {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	if (materialized.collection.count != 10) {
		return "Incorrect amount of rows in result";
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Join with hashtable.");
}
FINISH_BENCHMARK(HashJoinBenno)





DUCKDB_BENCHMARK(HashJoinOneMatch, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE t1 (v1 INTEGER)");
	state->conn.Query("CREATE TABLE t2 (v1 INTEGER)");
	vector<int32_t> values;
	for (int32_t i {}; i < 1000000; i ++){
		values.push_back(i);
		state->conn.Query("INSERT INTO integers t1 ($1)",i);
	}
	shuffle(values.begin(),values.end(), std::mt19937(std::random_device()()));
	for (idx_t i {}; i < values.size(); i ++){
		state->conn.Query("INSERT INTO integers t1 ($1)",values[i]);
	}
}

virtual string GetQuery() {
	return "SELECT count(*) from t1 inner join t2 on (t1.v1 = t2.v1)" ;
}

virtual string VerifyResult(QueryResult *result) {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	if (materialized.collection.count != 1) {
		return "Incorrect amount of rows in result";
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Join with hashtable.");
}
FINISH_BENCHMARK(HashJoinOneMatch)

DUCKDB_BENCHMARK(IndexJoinBenno, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE words(index INTEGER, doc INTEGER, word VARCHAR);");
	state->conn.Query("CREATE INDEX i_index ON words(word)");
	state->conn.Query("COPY words FROM 'benchmark/micro/index/indexjoin.csv.gz' (DELIMITER ',' , AUTO_DETECT FALSE)");
}

virtual string GetQuery() {
	return "SELECT w2.doc as doc2, COUNT(*) AS c FROM words AS w1 JOIN words AS w2 ON "
	       "(w1.word=w2.word) GROUP BY doc2 ORDER BY c DESC LIMIT 10";
}

virtual string VerifyResult(QueryResult *result) {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	if (materialized.collection.count != 10) {
		return "Incorrect amount of rows in result";
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Join with index.");
}
FINISH_BENCHMARK(IndexJoinBenno)

DUCKDB_BENCHMARK(IndexJoinOneMatch, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE t1 (v1 INTEGER)");
	state->conn.Query("CREATE INDEX i_index ON t1 using art(v1)" );
	state->conn.Query("CREATE TABLE t2 (v1 INTEGER)");
	vector<int32_t> values;
	for (int32_t i {}; i < 10000000; i ++){
		values.push_back(i);
		state->conn.Query("INSERT INTO integers t1 ($1)",i);
	}
	shuffle(values.begin(),values.end(), std::mt19937(std::random_device()()));
	for (idx_t i {}; i < values.size(); i ++){
		state->conn.Query("INSERT INTO integers t1 ($1)",values[i]);
	}
}

virtual string GetQuery() {
	return "SELECT count(*) from t1 inner join t2 on (t1.v1 = t2.v1)" ;
}

virtual string VerifyResult(QueryResult *result) {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	if (materialized.collection.count != 1) {
		return "Incorrect amount of rows in result";
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Join with hashtable.");
}
FINISH_BENCHMARK(IndexJoinOneMatch)
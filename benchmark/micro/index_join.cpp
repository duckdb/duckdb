#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

#include <random>

using namespace duckdb;
using namespace std;

DUCKDB_BENCHMARK(HashJoin, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE words(index INTEGER, doc INTEGER, word VARCHAR);");
	state->conn.Query("COPY words FROM 'benchmark/micro/index/indexjoin.csv.gz' (DELIMITER ',' , AUTO_DETECT FALSE)");
}

virtual string GetQuery() {
	return "SELECT w1.doc AS doc1, w2.doc as doc2, COUNT(*) AS c FROM words AS w1 JOIN words AS w2 ON "
	       "(w1.word=w2.word) GROUP BY doc1, doc2 ORDER BY c DESC LIMIT 10";
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
FINISH_BENCHMARK(HashJoin)

DUCKDB_BENCHMARK(IndexJoin, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE words(index INTEGER, doc INTEGER, word VARCHAR);");
	state->conn.Query("CREATE INDEX i_index ON words(word)");
	state->conn.Query("COPY words FROM 'benchmark/micro/index/indexjoin.csv.gz' (DELIMITER ',' , AUTO_DETECT FALSE)");
}

virtual string GetQuery() {
	return "SELECT w1.doc AS doc1, w2.doc as doc2, COUNT(*) AS c FROM words AS w1 JOIN words AS w2 ON "
	       "(w1.word=w2.word) GROUP BY doc1, doc2 ORDER BY c DESC LIMIT 10";
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
FINISH_BENCHMARK(IndexJoin)

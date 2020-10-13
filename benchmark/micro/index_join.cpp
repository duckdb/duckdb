#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

#include <random>
#include <algorithm>
#include <iostream>
using namespace duckdb;
using namespace std;

idx_t t1_size = 1000;
idx_t t2_size = 10000000;

DUCKDB_BENCHMARK(HashJoinBennoNoRHSFetch, "[micro]")
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
FINISH_BENCHMARK(HashJoinBennoNoRHSFetch)




DUCKDB_BENCHMARK(HashJoinHighCardinalityRHS, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE t1 (v1 INTEGER,v2 INTEGER)");
	state->conn.Query("CREATE TABLE t2 (v1 INTEGER,v2 INTEGER)");
	vector<int32_t> values;
	Appender appender_t1(state->conn, "t1");
	Appender appender_t2(state->conn, "t2");

	for (int32_t i {}; i < t2_size; i ++){
		values.push_back(i);
		if (i < t1_size){
			appender_t1.BeginRow();
			appender_t1.Append<int32_t>(i);
			appender_t1.Append<int32_t>(i);
			appender_t1.EndRow();
		}
	}
	shuffle(values.begin(),values.end(), std::mt19937(std::random_device()()));
	for (int32_t i {}; i < values.size(); i ++){
		appender_t2.BeginRow();
		appender_t2.Append<int32_t>(values[i]);
		appender_t2.Append<int32_t>(i);
		appender_t2.EndRow();
	}
}

virtual string GetQuery() {
	return "SELECT t1.v2,t2.v2,count(*) from t1 inner join t2 on (t1.v1 = t2.v1) group by t1.v2,t2.v2 order by t1.v2 limit 5" ;
}

virtual string VerifyResult(QueryResult *result) {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	cerr << materialized.collection.count << endl;
	if (materialized.collection.count != 5) {
		return to_string(materialized.collection.count);
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Join with hashtable.");
}
FINISH_BENCHMARK(HashJoinHighCardinalityRHS)

DUCKDB_BENCHMARK(HashJoinSameCardinalityLHSRHS, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE t1 (v1 INTEGER,v2 INTEGER)");
	state->conn.Query("CREATE TABLE t2 (v1 INTEGER,v2 INTEGER)");
	vector<int32_t> values;
	Appender appender_t1(state->conn, "t1");
	Appender appender_t2(state->conn, "t2");

	for (int32_t i {}; i < t2_size; i ++){
		values.push_back(i);
			appender_t1.BeginRow();
			appender_t1.Append<int32_t>(i);
			appender_t1.Append<int32_t>(i);
		    appender_t1.EndRow();

	}
	shuffle(values.begin(),values.end(), std::mt19937(std::random_device()()));
	for (int32_t i {}; i < values.size(); i ++){
		appender_t2.BeginRow();
		appender_t2.Append<int32_t>(values[i]);
		appender_t2.Append<int32_t>(i);
		appender_t2.EndRow();
	}
}

virtual string GetQuery() {
	return "SELECT t1.v2,t2.v2,count(*) from t1 inner join t2 on (t1.v1 = t2.v1) group by t1.v2,t2.v2 order by t1.v2 limit 5" ;
}

virtual string VerifyResult(QueryResult *result) {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	cerr << materialized.collection.count << endl;
	if (materialized.collection.count != 5) {
		return to_string(materialized.collection.count);
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Join with hashtable.");
}
FINISH_BENCHMARK(HashJoinSameCardinalityLHSRHS)

DUCKDB_BENCHMARK(IndexJoinBennoNoRHSFetch, "[micro]")
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
FINISH_BENCHMARK(IndexJoinBennoNoRHSFetch)

DUCKDB_BENCHMARK(IndexJoinHighCardinalityRHS, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE t1 (v1 INTEGER,v2 INTEGER)");
	state->conn.Query("CREATE TABLE t2 (v1 INTEGER,v2 INTEGER)");
	state->conn.Query("CREATE INDEX i_index ON t2(v1)");
	vector<int32_t> values;
	Appender appender_t1(state->conn, "t1");
	Appender appender_t2(state->conn, "t2");

	for (int32_t i {}; i < t2_size; i ++){
		values.push_back(i);
		if (i < t1_size){
			appender_t1.BeginRow();
			appender_t1.Append<int32_t>(i);
			appender_t1.Append<int32_t>(i);
			appender_t1.EndRow();
		}
	}
	shuffle(values.begin(),values.end(), std::mt19937(std::random_device()()));
	for (int32_t i {}; i < values.size(); i ++){
		appender_t2.BeginRow();
		appender_t2.Append<int32_t>(values[i]);
		appender_t2.Append<int32_t>(i);
		appender_t2.EndRow();
	}
}

virtual string GetQuery() {
	return "SELECT t1.v2,t2.v2,count(*) from t1 inner join t2 on (t1.v1 = t2.v1) group by t1.v2,t2.v2 order by t1.v2 limit 5" ;
}

virtual string VerifyResult(QueryResult *result) {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	if (materialized.collection.count != 5) {
		return "Incorrect amount of rows in result";
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Join with hashtable.");
}
FINISH_BENCHMARK(IndexJoinHighCardinalityRHS)


DUCKDB_BENCHMARK(HashJoinLHSArithmeticOperations, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE t1 (v1 INTEGER,v2 INTEGER)");
	state->conn.Query("CREATE TABLE t2 (v1 INTEGER,v2 INTEGER)");
	vector<int32_t> values;
	Appender appender_t1(state->conn, "t1");
	Appender appender_t2(state->conn, "t2");

	for (int32_t i {}; i < t2_size; i ++){
		values.push_back(i);
		if (i < t1_size*10){
			appender_t1.BeginRow();
			appender_t1.Append<int32_t>(i);
			appender_t1.Append<int32_t>(i);
			appender_t1.EndRow();
		}

	}
	shuffle(values.begin(),values.end(), std::mt19937(std::random_device()()));
	for (int32_t i {}; i < values.size(); i ++){
		appender_t2.BeginRow();
		appender_t2.Append<int32_t>(values[i]);
		appender_t2.Append<int32_t>(i);
		appender_t2.EndRow();
	}
}

virtual string GetQuery() {
	return "SELECT CASE WHEN t1.v1 > 50 THEN t1.v1+t1.v2 ELSE t1.v1*t1.v2 END FROM t1 JOIN t2 USING (v1);" ;
}

virtual string VerifyResult(QueryResult *result) {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	cerr << materialized.collection.count << endl;
	if (materialized.collection.count != 10000) {
		return to_string(materialized.collection.count);
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Join with hashtable.");
}
FINISH_BENCHMARK(HashJoinLHSArithmeticOperations)

DUCKDB_BENCHMARK(IndexJoinLHSArithmeticOperations, "[micro]")
virtual void Load(DuckDBBenchmarkState *state) {
	state->conn.Query("CREATE TABLE t1 (v1 INTEGER,v2 INTEGER)");
	state->conn.Query("CREATE TABLE t2 (v1 INTEGER,v2 INTEGER)");
	state->conn.Query("CREATE INDEX i_index ON t2(v1)");
	vector<int32_t> values;
	Appender appender_t1(state->conn, "t1");
	Appender appender_t2(state->conn, "t2");

	for (int32_t i {}; i < t2_size; i ++){
		values.push_back(i);
		if (i < t1_size*10){
			appender_t1.BeginRow();
			appender_t1.Append<int32_t>(i);
			appender_t1.Append<int32_t>(i);
			appender_t1.EndRow();
		}

	}
	shuffle(values.begin(),values.end(), std::mt19937(std::random_device()()));
	for (int32_t i {}; i < values.size(); i ++){
		appender_t2.BeginRow();
		appender_t2.Append<int32_t>(values[i]);
		appender_t2.Append<int32_t>(i);
		appender_t2.EndRow();
	}
}

virtual string GetQuery() {
	return "SELECT CASE WHEN t1.v1 > 50 THEN t1.v1+t1.v2 ELSE t1.v1*t1.v2 END FROM t1 JOIN t2 USING (v1);" ;
}

virtual string VerifyResult(QueryResult *result) {
	if (!result->success) {
		return result->error;
	}
	auto &materialized = (MaterializedQueryResult &)*result;
	cerr << materialized.collection.count << endl;
	if (materialized.collection.count != 10000) {
		return to_string(materialized.collection.count);
	}
	return string();
}

virtual string BenchmarkInfo() {
	return StringUtil::Format("Join with hashtable.");
}
FINISH_BENCHMARK(IndexJoinLHSArithmeticOperations)
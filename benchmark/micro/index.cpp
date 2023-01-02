#include "benchmark_runner.hpp"
#include "duckdb_benchmark_macro.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;

DUCKDB_BENCHMARK(CreateIndexIntSorted, "[createindexintsorted]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE art AS SELECT range id FROM range(1000000);");
}
void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE INDEX idx ON art(id);");
}
void Cleanup(DuckDBBenchmarkState *state) override {
	state->conn.Query("DROP INDEX idx;");
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Run a CREATE INDEX statement.";
}
FINISH_BENCHMARK(CreateIndexIntSorted)

DUCKDB_BENCHMARK(CreateIndexIntRandom, "[createindexintrandom]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE art AS SELECT (random() * 1000000)::INT AS id FROM range(100000000);");
}
void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE INDEX idx ON art(id);");
}
void Cleanup(DuckDBBenchmarkState *state) override {
	state->conn.Query("DROP INDEX idx;");
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Run a CREATE INDEX statement.";
}
FINISH_BENCHMARK(CreateIndexIntRandom)

DUCKDB_BENCHMARK(CreateIndexIntDuplicates, "[createindexintduplicates]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE art AS SELECT (random() * 100)::INT AS id FROM range(100000000);");
}
void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE INDEX idx ON art(id);");
}
void Cleanup(DuckDBBenchmarkState *state) override {
	state->conn.Query("DROP INDEX idx;");
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Run a CREATE INDEX statement.";
}
FINISH_BENCHMARK(CreateIndexIntDuplicates)

DUCKDB_BENCHMARK(CreateIndexVarchar, "[createindexvarchar]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE art AS SELECT range || '-not-inlined-' id FROM range(10000000);");
}
void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE INDEX idx ON art(id);");
}
void Cleanup(DuckDBBenchmarkState *state) override {
	state->conn.Query("DROP INDEX idx;");
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Run a CREATE INDEX statement.";
}
FINISH_BENCHMARK(CreateIndexVarchar)

DUCKDB_BENCHMARK(InsertIndexUnique, "[insertindexunique]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE art (id INTEGER);");
	state->conn.Query("CREATE UNIQUE INDEX idx ON art(id);");
}
void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("INSERT INTO art (SELECT range id FROM range(10000000));");
}
void Cleanup(DuckDBBenchmarkState *state) override {
	state->conn.Query("DELETE FROM art;");
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Create an index and insert data.";
}
FINISH_BENCHMARK(InsertIndexUnique)

DUCKDB_BENCHMARK(InsertIndex, "[insertindex]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("CREATE TABLE art (id INTEGER);");
	state->conn.Query("CREATE INDEX idx ON art(id);");
}
void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("INSERT INTO art (SELECT range id FROM range(10000000));");
}
void Cleanup(DuckDBBenchmarkState *state) override {
	state->conn.Query("DELETE FROM art;");
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Create an index and insert data.";
}
FINISH_BENCHMARK(InsertIndex)

DUCKDB_BENCHMARK(IndexJoin, "[indexjoin]")
void Load(DuckDBBenchmarkState *state) override {
	state->conn.Query("PRAGMA enable_verification;");
	state->conn.Query("PRAGMA force_index_join;");
	state->conn.Query("CREATE TABLE Person (id bigint PRIMARY KEY);");
	state->conn.Query("CREATE TABLE Person_knows_Person (Person1id bigint, Person2id bigint);");
	state->conn.Query("INSERT INTO Person SELECT range id FROM range(100000);");
	state->conn.Query(
	    "INSERT INTO Person_knows_Person SELECT range AS Person1id, range + 1 AS Person2id FROM range(99999);");
	state->conn.Query(
	    "INSERT INTO Person_knows_Person SELECT range AS Person1id, range + 5 AS Person2id FROM range(99995);");
}
void RunBenchmark(DuckDBBenchmarkState *state) override {
	state->conn.Query("SELECT p1.id FROM Person_knows_Person pkp JOIN Person p1 ON p1.id = pkp.Person1id;");
}
string VerifyResult(QueryResult *result) override {
	return string();
}
string BenchmarkInfo() override {
	return "Run an index join statement.";
}
FINISH_BENCHMARK(IndexJoin)

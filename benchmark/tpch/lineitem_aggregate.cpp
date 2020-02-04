#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 1

DUCKDB_BENCHMARK(LineitemSimpleAggregate, "[aggregate]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT SUM(l_quantity) FROM lineitem";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute the query \"SELECT SUM(l_quantity) FROM lineitem\" on SF1";
}
FINISH_BENCHMARK(LineitemSimpleAggregate)

DUCKDB_BENCHMARK(LineitemCount, "[aggregate]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT COUNT(*) FROM lineitem";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute the query \"SELECT COUNT(*) FROM lineitem\" on SF1";
}
FINISH_BENCHMARK(LineitemCount)

DUCKDB_BENCHMARK(LineitemGroupAggregate, "[aggregate]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT l_quantity, SUM(l_quantity) FROM lineitem GROUP BY l_quantity";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute the query \"SELECT l_quantity, SUM(l_quantity) FROM lineitem GROUP BY l_quantity\" on SF1";
}
FINISH_BENCHMARK(LineitemGroupAggregate)

DUCKDB_BENCHMARK(LineitemGroupStringAggregate, "[aggregate]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT SUM(l_quantity) FROM lineitem GROUP BY l_returnflag";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute the query \"SELECT SUM(l_quantity) FROM lineitem GROUP BY l_returnflag\" on SF1";
}
FINISH_BENCHMARK(LineitemGroupStringAggregate)

DUCKDB_BENCHMARK(LineitemJoinAggregate, "[aggregate]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, "
	       "sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + "
	       "l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS "
	       "avg_disc, count(*) AS count_order FROM lineitem, orders WHERE l_orderkey=o_orderkey GROUP BY l_returnflag, "
	       "l_linestatus ORDER BY l_returnflag, l_linestatus";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute the query \"" + GetQuery() + "\" on SF1";
}
FINISH_BENCHMARK(LineitemJoinAggregate)

DUCKDB_BENCHMARK(LineitemJoinAggregateWithFilter, "[aggregate]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db);
}
string GetQuery() override {
	return "SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, "
	       "sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + "
	       "l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS "
	       "avg_disc, count(*) AS count_order FROM lineitem, orders WHERE l_orderkey=o_orderkey AND l_shipdate <= "
	       "cast('1998-09-02' AS date) GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus";
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute the query \"" + GetQuery() + "\" on SF1";
}
FINISH_BENCHMARK(LineitemJoinAggregateWithFilter)

DUCKDB_BENCHMARK(TPCHQ1IntKeys, "[aggregate]")
void Load(DuckDBBenchmarkState *state) override {
	// load the data into the tpch schema
	tpch::dbgen(SF, state->db, DEFAULT_SCHEMA, "_normal");
	Connection conn(state->db);
	conn.Query("CREATE TABLE lineitem AS select l_orderkey, l_partkey,  l_suppkey,    l_linenumber,    l_quantity,    "
	           "l_extendedprice,    l_discount,    l_tax, case l_returnflag when 'N' then 0 when 'R' then 1 when 'A' "
	           "then 2 else NULL end l_returnflag, case l_linestatus when 'F' then 0 when 'O' then 1 else NULL end "
	           "l_linestatus, l_shipdate,     l_commitdate,     l_receiptdate,   l_shipinstruct,    l_shipmode,    "
	           "l_comment from lineitem_normal");
}
string GetQuery() override {
	return tpch::get_query(1);
}
string VerifyResult(QueryResult *result) override {
	if (!result->success) {
		return result->error;
	}
	return string();
}
string BenchmarkInfo() override {
	return "Execute the query \"" + GetQuery() + "\" on SF1";
}
FINISH_BENCHMARK(TPCHQ1IntKeys)

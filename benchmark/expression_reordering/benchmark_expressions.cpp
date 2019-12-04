#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 0.5

string getQuery(int queryID) {
		
	string queries [36] = {
		"l_quantity <= 1 + 10",
		"l_shipdate > date '1992-01-01'",
		"l_receiptdate >= l_commitdate",
		"l_shipinstruct <= l_comment",

		"l_commitdate = l_receiptdate",
		"l_orderkey != 0",
		"l_discount = 1.0",
		"l_returnflag = 'R'",
		"l_shipinstruct != 'DELIVER IN PERSON'",

		"l_returnflag IS NOT NULL",
		"l_quantity IS NOT NULL",

		"l_shipinstruct LIKE 'DE%I%ER%'",
		"l_comment NOT LIKE '%str%'",
		"l_shipinstruct SIMILAR TO 'DE.*I.*ER.*'",
		"l_comment NOT SIMILAR TO '.*str.*'",

		"l_shipmode IN ('MAIL', 'SHIP')",

		"(CASE WHEN l_orderkey = 2 THEN 1 ELSE 0 END) = 1",

		"l_discount + l_tax",
		"l_tax - l_discount",
		"l_receiptdate - l_commitdate",
		"l_discount * l_tax",
		"l_discount / l_tax",
		"l_orderkey % 5",
		"l_orderkey & l_partkey",
		"l_orderkey # l_partkey",
		"l_orderkey >> l_partkey",
		"l_orderkey << l_partkey",

		"abs(l_extendedprice)",
		"round(l_discount, 1)",

		"l_shipinstruct || l_returnflag = 'R'",
		"length(l_comment)",
		"lower(l_comment) = 'R'",
		"upper(l_comment) = 'R'",
		"substring(l_shipinstruct, 1, 7) = 'R'",

		"date_part('year', l_commitdate)",
		"age(l_commitdate::TIMESTAMP)"
		};

	string enable_profiling = "pragma enable_profiling='json';";
	string query = "SELECT * FROM lineitem WHERE " + queries[queryID] + ";";
	return enable_profiling + query;
}

#define HEURISTICS_QUERY_BODY(QNR)                                                                                     \
	virtual void Load(DuckDBBenchmarkState *state) {                                                                   \
		tpch::dbgen(SF, state->db);                        		                                                       \
	}                                                                                                                  \
	virtual string GetQuery() {                                                                                        \
		return getQuery(QNR);                                                                                   	   \
	}                                                                                                                  \
	virtual string VerifyResult(QueryResult *result) {                                                                 \
		if (!result->success) {                                                                                        \
			return result->error;                                                                                      \
		}                                                                                                              \
		return string();                                                											   \
	}                                                                                                                  \
	virtual string BenchmarkInfo() {                                                                                   \
		return "Executing heuristics query...";                        												   \
	}

DUCKDB_BENCHMARK(ExpressionReorderingH0, "[expression_reordering]")
HEURISTICS_QUERY_BODY(0);
FINISH_BENCHMARK(ExpressionReorderingH0)

DUCKDB_BENCHMARK(ExpressionReorderingH1, "[expression_reordering]")
HEURISTICS_QUERY_BODY(1);
FINISH_BENCHMARK(ExpressionReorderingH1)

DUCKDB_BENCHMARK(ExpressionReorderingH2, "[expression_reordering]")
HEURISTICS_QUERY_BODY(2);
FINISH_BENCHMARK(ExpressionReorderingH2)

DUCKDB_BENCHMARK(ExpressionReorderingH3, "[expression_reordering]")
HEURISTICS_QUERY_BODY(3);
FINISH_BENCHMARK(ExpressionReorderingH3)

DUCKDB_BENCHMARK(ExpressionReorderingH4, "[expression_reordering]")
HEURISTICS_QUERY_BODY(4);
FINISH_BENCHMARK(ExpressionReorderingH4)

DUCKDB_BENCHMARK(ExpressionReorderingH5, "[expression_reordering]")
HEURISTICS_QUERY_BODY(5);
FINISH_BENCHMARK(ExpressionReorderingH5)

DUCKDB_BENCHMARK(ExpressionReorderingH6, "[expression_reordering]")
HEURISTICS_QUERY_BODY(6);
FINISH_BENCHMARK(ExpressionReorderingH6)

DUCKDB_BENCHMARK(ExpressionReorderingH7, "[expression_reordering]")
HEURISTICS_QUERY_BODY(7);
FINISH_BENCHMARK(ExpressionReorderingH7)

DUCKDB_BENCHMARK(ExpressionReorderingH8, "[expression_reordering]")
HEURISTICS_QUERY_BODY(8);
FINISH_BENCHMARK(ExpressionReorderingH8)

DUCKDB_BENCHMARK(ExpressionReorderingH9, "[expression_reordering]")
HEURISTICS_QUERY_BODY(9);
FINISH_BENCHMARK(ExpressionReorderingH9)

DUCKDB_BENCHMARK(ExpressionReorderingH10, "[expression_reordering]")
HEURISTICS_QUERY_BODY(10);
FINISH_BENCHMARK(ExpressionReorderingH10)

DUCKDB_BENCHMARK(ExpressionReorderingH11, "[expression_reordering]")
HEURISTICS_QUERY_BODY(11);
FINISH_BENCHMARK(ExpressionReorderingH11)

DUCKDB_BENCHMARK(ExpressionReorderingH12, "[expression_reordering]")
HEURISTICS_QUERY_BODY(12);
FINISH_BENCHMARK(ExpressionReorderingH12)

DUCKDB_BENCHMARK(ExpressionReorderingH13, "[expression_reordering]")
HEURISTICS_QUERY_BODY(13);
FINISH_BENCHMARK(ExpressionReorderingH13)

DUCKDB_BENCHMARK(ExpressionReorderingH14, "[expression_reordering]")
HEURISTICS_QUERY_BODY(14);
FINISH_BENCHMARK(ExpressionReorderingH14)

DUCKDB_BENCHMARK(ExpressionReorderingH15, "[expression_reordering]")
HEURISTICS_QUERY_BODY(15);
FINISH_BENCHMARK(ExpressionReorderingH15)

DUCKDB_BENCHMARK(ExpressionReorderingH16, "[expression_reordering]")
HEURISTICS_QUERY_BODY(16);
FINISH_BENCHMARK(ExpressionReorderingH16)

DUCKDB_BENCHMARK(ExpressionReorderingH17, "[expression_reordering]")
HEURISTICS_QUERY_BODY(17);
FINISH_BENCHMARK(ExpressionReorderingH17)

DUCKDB_BENCHMARK(ExpressionReorderingH18, "[expression_reordering]")
HEURISTICS_QUERY_BODY(18);
FINISH_BENCHMARK(ExpressionReorderingH18)

DUCKDB_BENCHMARK(ExpressionReorderingH19, "[expression_reordering]")
HEURISTICS_QUERY_BODY(19);
FINISH_BENCHMARK(ExpressionReorderingH19)

DUCKDB_BENCHMARK(ExpressionReorderingH20, "[expression_reordering]")
HEURISTICS_QUERY_BODY(20);
FINISH_BENCHMARK(ExpressionReorderingH20)

DUCKDB_BENCHMARK(ExpressionReorderingH21, "[expression_reordering]")
HEURISTICS_QUERY_BODY(21);
FINISH_BENCHMARK(ExpressionReorderingH21)

DUCKDB_BENCHMARK(ExpressionReorderingH22, "[expression_reordering]")
HEURISTICS_QUERY_BODY(22);
FINISH_BENCHMARK(ExpressionReorderingH22)

DUCKDB_BENCHMARK(ExpressionReorderingH23, "[expression_reordering]")
HEURISTICS_QUERY_BODY(23);
FINISH_BENCHMARK(ExpressionReorderingH23)

DUCKDB_BENCHMARK(ExpressionReorderingH24, "[expression_reordering]")
HEURISTICS_QUERY_BODY(24);
FINISH_BENCHMARK(ExpressionReorderingH24)

DUCKDB_BENCHMARK(ExpressionReorderingH25, "[expression_reordering]")
HEURISTICS_QUERY_BODY(25);
FINISH_BENCHMARK(ExpressionReorderingH25)

DUCKDB_BENCHMARK(ExpressionReorderingH26, "[expression_reordering]")
HEURISTICS_QUERY_BODY(26);
FINISH_BENCHMARK(ExpressionReorderingH26)

DUCKDB_BENCHMARK(ExpressionReorderingH27, "[expression_reordering]")
HEURISTICS_QUERY_BODY(27);
FINISH_BENCHMARK(ExpressionReorderingH27)

DUCKDB_BENCHMARK(ExpressionReorderingH28, "[expression_reordering]")
HEURISTICS_QUERY_BODY(28);
FINISH_BENCHMARK(ExpressionReorderingH28)

DUCKDB_BENCHMARK(ExpressionReorderingH29, "[expression_reordering]")
HEURISTICS_QUERY_BODY(29);
FINISH_BENCHMARK(ExpressionReorderingH29)

DUCKDB_BENCHMARK(ExpressionReorderingH30, "[expression_reordering]")
HEURISTICS_QUERY_BODY(30);
FINISH_BENCHMARK(ExpressionReorderingH30)

DUCKDB_BENCHMARK(ExpressionReorderingH31, "[expression_reordering]")
HEURISTICS_QUERY_BODY(31);
FINISH_BENCHMARK(ExpressionReorderingH31)

DUCKDB_BENCHMARK(ExpressionReorderingH32, "[expression_reordering]")
HEURISTICS_QUERY_BODY(32);
FINISH_BENCHMARK(ExpressionReorderingH32)

DUCKDB_BENCHMARK(ExpressionReorderingH33, "[expression_reordering]")
HEURISTICS_QUERY_BODY(33);
FINISH_BENCHMARK(ExpressionReorderingH33)

DUCKDB_BENCHMARK(ExpressionReorderingH34, "[expression_reordering]")
HEURISTICS_QUERY_BODY(34);
FINISH_BENCHMARK(ExpressionReorderingH34)

DUCKDB_BENCHMARK(ExpressionReorderingH35, "[expression_reordering]")
HEURISTICS_QUERY_BODY(35);
FINISH_BENCHMARK(ExpressionReorderingH35)
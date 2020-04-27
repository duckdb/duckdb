#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 0.2

string getQuery(int queryID) {

	string queries[42] = {
	    "l_quantity <= 1 + 10",           // H0, BOUND_COMPARISON, COMPARE_LESSTHANOREQUALTO
	    "l_shipdate > date '1992-01-01'", // H1, BOUND_COMPARISON, COMPARE_GREATERTHAN
	    "l_receiptdate >= l_commitdate",  // H2, BOUND_COMPARISON, COMPARE_GREATERTHANOREQUALTO
	    "l_shipinstruct < l_comment",     // H3, BOUND_COMPARISON,

	    "l_commitdate = l_receiptdate",          // H4, BOUND_COMPARISON, COMPARE_EQUAL
	    "l_orderkey != 0",                       // H5, BOUND_COMPARISON, COMPARE_NOTEQUAL
	    "l_discount = 1.0",                      // H6, BOUND_COMPARISON, COMPARE_EQUAL
	    "l_returnflag = 'R'",                    // H7, BOUND_COMPARISON, COMPARE_EQUAL
	    "l_shipinstruct != 'DELIVER IN PERSON'", // H8, BOUND_COMPARISON, COMPARE_NOTEQUAL

	    "l_returnflag IS NOT NULL", // H9, BOUND_OPERATOR, OPERATOR_IS_NOT_NULL
	    "l_quantity IS NULL",       // H10, BOUND_OPERATOR,

	    "l_shipinstruct LIKE 'DE%I%ER%'",          // H11, BOUND_FUNCTION, "~~"
	    "l_comment NOT LIKE '%str%'",              // H12, BOUND_FUNCTION, "!~~"
	    "l_shipinstruct SIMILAR TO 'DE.*I.*ER.*'", // H13, BOUND_FUNCTION, "regexp_matches"
	    "l_comment NOT SIMILAR TO '.*str.*'",      // H14, BOUND_FUNCTION, "regexp_matches", preceeded by NOT

	    "l_shipmode IN ('MAIL', 'SHIP')", // H15, BOUND_OPERATOR, COMPARE_IN, children - 1 is the cardinality, loop over
	                                      // children

	    "(CASE WHEN l_orderkey = 2 THEN 1 ELSE 0 END) = 1", // H16, BOUND_CASE, three children

	    "l_discount + l_tax",           // H17, BOUND_FUNCTION, "+"
	    "l_tax - l_discount",           // H18, BOUND_FUNCTION, "-"
	    "l_receiptdate - l_commitdate", // H19, BOUND_FUNCTION, "-"
	    "l_discount * l_tax",           // H20, BOUND_FUNCTION, "*"
	    "l_discount / l_tax",           // H21, BOUND_FUNCTION, "/"
	    "l_orderkey % 5",               // H22, BOUND_FUNCTION, "%"
	    "l_orderkey & l_partkey",       // H23, BOUND_FUNCTION, "&"
	    "l_orderkey # l_partkey",       // H24, BOUND_FUNCTION, "#"
	    "l_orderkey >> l_partkey",      // H25, BOUND_FUNCTION, ">>"
	    "l_orderkey << l_partkey",      // H26, BOUND_FUNCTION, "<<"

	    "abs(l_extendedprice)", // H27, BOUND_FUNCTION, "abs"
	    "round(l_discount, 1)", // H28, BOUND_FUNCTION, "round"

	    "l_shipinstruct || l_returnflag = 'R'",  // H29, BOUND_FUNCTION, "||"
	    "length(l_comment)",                     // H30, BOUND_FUNCTION, "length"
	    "lower(l_comment) = 'R'",                // H31, BOUND_FUNCTION, "lower"
	    "upper(l_comment) = 'R'",                // H32, BOUND_FUNCTION, "upper"
	    "substring(l_shipinstruct, 1, 7) = 'R'", // H33, BOUND_FUNCTION, "substring"

	    "date_part('year', l_commitdate)", // H34, BOUND_FUNCTION, "date_part"

	    "l_orderkey::VARCHAR = '1'", // H35, BOUND_CAST, OPERATOR_CAST
	    "l_orderkey::DOUBLE = 3.0",  // H36, BOUND_CAST, OPERATOR_CAST

	    "l_comment SIMILAR TO '.*str.*'",     // H37, BOUND_FUNCTION, used to estimate runtime of OPERATOR_NOT
	    "l_quantity < 10 OR l_quantity > 20", // H38, BOUND_CONJUNCTION, CONJUNCTION_OR
	    "(l_quantity < 10 AND l_shipdate > date '1992-01-01') OR l_quantity > 20", // H39, BOUND_CONJUNCTION,
	                                                                               // CONJUNCTION_AND
	    "EXTRACT(YEAR from l_shipdate) = 1995", // H40, BOUND_FUNCTION, "date_part"
	    "YEAR(l_shipdate) IN ('1995','1996')"   // H41, BOUND_FUNCTION, "year"
	};

	string enable_profiling = "pragma enable_profiling='json';";
	string query = "SELECT * FROM lineitem WHERE " + queries[queryID] + ";";
	return enable_profiling + query;
}

#define HEURISTICS_QUERY_BODY(QNR)                                                                                     \
	virtual void Load(DuckDBBenchmarkState *state) {                                                                   \
		tpch::dbgen(SF, state->db);                                                                                    \
	}                                                                                                                  \
	virtual string GetQuery() {                                                                                        \
		return getQuery(QNR);                                                                                          \
	}                                                                                                                  \
	virtual string VerifyResult(QueryResult *result) {                                                                 \
		if (!result->success) {                                                                                        \
			return result->error;                                                                                      \
		}                                                                                                              \
		return string();                                                                                               \
	}                                                                                                                  \
	virtual string BenchmarkInfo() {                                                                                   \
		return "Executing heuristics query...";                                                                        \
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

DUCKDB_BENCHMARK(ExpressionReorderingH36, "[expression_reordering]")
HEURISTICS_QUERY_BODY(36);
FINISH_BENCHMARK(ExpressionReorderingH36)

DUCKDB_BENCHMARK(ExpressionReorderingH37, "[expression_reordering]")
HEURISTICS_QUERY_BODY(37);
FINISH_BENCHMARK(ExpressionReorderingH37)

DUCKDB_BENCHMARK(ExpressionReorderingH38, "[expression_reordering]")
HEURISTICS_QUERY_BODY(38);
FINISH_BENCHMARK(ExpressionReorderingH38)

DUCKDB_BENCHMARK(ExpressionReorderingH39, "[expression_reordering]")
HEURISTICS_QUERY_BODY(39);
FINISH_BENCHMARK(ExpressionReorderingH39)

DUCKDB_BENCHMARK(ExpressionReorderingH40, "[expression_reordering]")
HEURISTICS_QUERY_BODY(40);
FINISH_BENCHMARK(ExpressionReorderingH40)

DUCKDB_BENCHMARK(ExpressionReorderingH41, "[expression_reordering]")
HEURISTICS_QUERY_BODY(41);
FINISH_BENCHMARK(ExpressionReorderingH41)

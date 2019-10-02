#include "benchmark_runner.hpp"
#include "compare_result.hpp"
#include "dbgen.hpp"
#include "duckdb_benchmark_macro.hpp"

using namespace duckdb;
using namespace std;

#define SF 1

#define FBORRI(NAME, QUERY)                                                                                            \
	DUCKDB_BENCHMARK(NAME, "[fborri]")                                                                                 \
	void Load(DuckDBBenchmarkState *state) override {                                                                  \
		tpch::dbgen(SF, state->db, DEFAULT_SCHEMA, "", true);                                                          \
	}                                                                                                                  \
	string GetQuery() override {                                                                                       \
		return QUERY;                                                                                                  \
	}                                                                                                                  \
	string VerifyResult(QueryResult *result) override {                                                                \
		if (!result->success) {                                                                                        \
			return result->error;                                                                                      \
		}                                                                                                              \
		return string();                                                                                               \
	}                                                                                                                  \
	string BenchmarkInfo() override {                                                                                  \
		return "Benchmark based on Orri's queries";                                                                    \
	}                                                                                                                  \
	bool GroupCacheState() override {                                                                                  \
		return true;                                                                                                   \
	}                                                                                                                  \
	FINISH_BENCHMARK(NAME)

FBORRI(FBOrri01, "select count(*) from lineitem where l_partkey between 2000000 and 4000000 and l_suppkey between "
                 "100000 and 200000");
FBORRI(FBOrri02,
       "select count(*) from lineitem where l_shipmode between 'A' and 'B' and l_comment between 'a' and 'f'");
FBORRI(FBOrri03, "select count(*) from lineitem where l_shipmode like '%AIR%' and l_comment like '%sly%'");
FBORRI(FBOrri04, "select avg(l_extendedprice * (1e0 - l_discount) * l_tax / l_quantity * (case when l_shipmode = "
                 "'TRUCK' then 0.9e0 when l_shipmode = 'REG-AIR' then 1.1e0 else 1e0 end)) from lineitem")
FBORRI(FBOrri05,
       "select max(l_extendedprice), max(l_discount), max(l_tax), max(l_quantity), max (l_shipmode) from lineitem")
	   FBORRI(FBOrri06,
			   tpch::get_query(1))
FBORRI(FBOrri07,
       "select l_partkey, count(*), sum(l_extendedprice) from lineitem group by l_partkey order by 3 desc limit 20")
FBORRI(FBOrri08, "select l_orderkey, sum(l_extendedprice) from lineitem group by l_orderkey order by 2 desc limit 20")
FBORRI(FBOrri09, tpch::get_query(4));
FBORRI(FBOrri10, tpch::get_query(6));
FBORRI(FBOrri11, tpch::get_query(9));
